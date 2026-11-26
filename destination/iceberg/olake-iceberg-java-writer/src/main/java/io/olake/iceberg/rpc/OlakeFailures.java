package io.olake.iceberg.rpc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;

import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Builds the gRPC error the Go side reads.
 *
 * <p>The status code is INTERNAL for everything raised here, and the description is prose the Go
 * side deliberately does not parse — it changes between library versions and can contain table
 * names and column values. The facts travel as an {@link ErrorInfo} detail instead, a standard
 * google.rpc type, so neither side's generated protobuf changes.
 *
 * <p>Only what was caught is sent, never what it means: the taxonomy lives in Go alone, so it
 * cannot drift between two languages.
 */
public final class OlakeFailures {

    /** Identifies these details as ours, so the Go side never reads someone else's ErrorInfo. */
    public static final String DOMAIN = "olake.iceberg";

    /** The underlying system's own code, where the exception carried one. */
    public static final String METADATA_CODE = "code";

    /** The call being served when the failure happened, e.g. GET_OR_CREATE_TABLE. */
    public static final String METADATA_OPERATION = "operation";

    /** The method that raised the root cause, as package.Class.method. */
    public static final String METADATA_ORIGIN = "origin";

    /** The HTTP status the catalog or object storage answered with, where the failure was one. */
    public static final String METADATA_HTTP_STATUS = "http_status";

    /** The error type a REST catalog named in its error response. */
    public static final String METADATA_SERVER_ERROR_TYPE = "server_error_type";

    /**
     * Every message in the cause chain, as raised. It can hold a credential or a record value,
     * so Go masks it before it leaves the process and it is never cut here: a cut could split one.
     */
    public static final String METADATA_MESSAGE = "message";

    /** The operation reported for a failure while the writer starts, before any call is served. */
    public static final String OPERATION_STARTUP = "STARTUP";

    private OlakeFailures() {
    }

    /**
     * Attached with addSuppressed to an exception raised for an HTTP error response, for clients
     * whose exceptions do not keep the response: its status, and the error type the server named.
     */
    public static final class HttpError extends RuntimeException {
        private final int status;
        private final String type;

        public HttpError(int status, String type) {
            super("HTTP " + status + (type == null ? "" : " " + type), null, false, false);
            this.status = status;
            this.type = type;
        }
    }

    /**
     * Wraps a caught exception as a gRPC exception carrying an ErrorInfo detail.
     *
     * @param cause     the exception that ended the call
     * @param operation the payload type being served, e.g. {@code GET_OR_CREATE_TABLE}
     * @param message   the description an operator reads; unchanged from what is logged
     */
    public static StatusRuntimeException toStatusException(Throwable cause, String operation, String message) {
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.INTERNAL.getNumber())
                .setMessage(message == null ? "" : message)
                .addDetails(Any.pack(describe(cause, operation)))
                .build();

        return StatusProto.toStatusRuntimeException(status);
    }

    /**
     * Writes the facts of a failure to start to path, for the Go side to read once the process has
     * exited: before the server is up there is no gRPC call to carry them. Best effort, since the
     * writer is already failing and the stderr line still names the class.
     */
    public static void writeStartupReport(Throwable cause, String path) {
        try {
            Files.write(Paths.get(path), describe(cause, OPERATION_STARTUP).toByteArray());
        } catch (IOException | RuntimeException e) {
            System.err.println("could not write the startup failure report: " + e);
        }
    }

    /** The facts Go classifies a failure on. Only what was caught, never what it means. */
    static ErrorInfo describe(Throwable cause, String operation) {
        Throwable root = rootCause(cause);
        ErrorInfo.Builder info = ErrorInfo.newBuilder()
                .setDomain(DOMAIN)
                .setReason(root.getClass().getName());

        if (operation != null && !operation.isEmpty()) {
            info.putMetadata(METADATA_OPERATION, operation);
        }
        String code = vendorCode(cause);
        if (code != null && !code.isEmpty()) {
            info.putMetadata(METADATA_CODE, code);
        }
        String origin = origin(root);
        if (origin != null) {
            info.putMetadata(METADATA_ORIGIN, origin);
        }
        HttpError httpError = httpError(cause);
        if (httpError != null) {
            info.putMetadata(METADATA_HTTP_STATUS, Integer.toString(httpError.status));
            if (httpError.type != null && !httpError.type.isEmpty()) {
                info.putMetadata(METADATA_SERVER_ERROR_TYPE, httpError.type);
            }
        }
        String messages = messages(cause);
        if (!messages.isEmpty()) {
            info.putMetadata(METADATA_MESSAGE, messages);
        }
        return info.build();
    }

    /**
     * Joins the messages of the cause chain, outermost first. A wrapper built from its cause
     * repeats the cause's message, so a message the one before it already contains is skipped.
     */
    private static String messages(Throwable cause) {
        StringBuilder chain = new StringBuilder();
        String last = "";
        // Bounded like rootCause, so a cycle of causes cannot loop.
        Throwable t = cause;
        for (int depth = 0; t != null && depth < 16; depth++, t = t.getCause()) {
            String message = t.getMessage();
            if (message != null && !message.isEmpty() && !last.contains(message)) {
                chain.append(chain.length() == 0 ? "" : " <- ").append(message);
                last = message;
            }
        }
        return chain.toString();
    }

    /**
     * Returns the innermost cause. Iceberg and Hadoop wrap heavily, and the outermost class is
     * usually a generic wrapper that identifies nothing. Public so the startup path can reuse it.
     */
    public static Throwable rootCause(Throwable t) {
        Throwable root = t;
        // Bounded so a self-referencing cause cannot loop.
        for (int depth = 0; depth < 16; depth++) {
            Throwable next = root.getCause();
            if (next == null || next == root) {
                break;
            }
            root = next;
        }
        return root;
    }

    /**
     * Names the method that raised t: the first stack frame outside t's own class, which skips
     * its builder. A class and method name from code, never a value. Null when the JVM dropped
     * the stack trace.
     */
    private static String origin(Throwable t) {
        String own = t.getClass().getName();
        for (StackTraceElement frame : t.getStackTrace()) {
            String frameClass = frame.getClassName();
            if (!frameClass.equals(own) && !frameClass.startsWith(own + "$")) {
                return frameClass + "." + frame.getMethodName();
            }
        }
        return null;
    }

    /**
     * Reports the underlying system's own error code where the exception carries one. Two systems
     * do: a JDBC catalog through SQLSTATE, and object storage through the S3 error code, which is
     * the only thing separating a wrong key from a missing bucket. Everything else is identified
     * by its exception class alone.
     */
    private static String vendorCode(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof SQLException) {
                return ((SQLException) t).getSQLState();
            }
            if (t instanceof AwsServiceException) {
                AwsErrorDetails details = ((AwsServiceException) t).awsErrorDetails();
                if (details != null) {
                    return details.errorCode();
                }
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }

    /**
     * Reports the HTTP response that failed, walking outward-in like vendorCode: from the status
     * an AWS SDK service exception keeps, or from an HttpError attached to the exception. Null
     * where the failure was not an HTTP response.
     */
    private static HttpError httpError(Throwable cause) {
        // Bounded like rootCause, so a cycle of two or more causes cannot loop.
        Throwable t = cause;
        for (int depth = 0; t != null && depth < 16; depth++) {
            if (t instanceof SdkServiceException && ((SdkServiceException) t).statusCode() > 0) {
                return new HttpError(((SdkServiceException) t).statusCode(), null);
            }
            for (Throwable suppressed : t.getSuppressed()) {
                if (suppressed instanceof HttpError) {
                    return (HttpError) suppressed;
                }
            }
            t = t.getCause() == t ? null : t.getCause();
        }
        return null;
    }
}
