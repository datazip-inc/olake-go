package io.olake.iceberg.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;

import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Pins the ErrorInfo payload Go parses. A change here is invisible to the Go compiler.
 */
class OlakeFailuresTest {

    /** Cause that unwraps to itself — both chain walks must terminate. */
    private static final class SelfReferencing extends RuntimeException {
        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }

    /** Unpacks the ErrorInfo Go will read, or fails if none was attached. */
    private static ErrorInfo errorInfoOf(StatusRuntimeException e) throws InvalidProtocolBufferException {
        Status status = StatusProto.fromThrowable(e);
        assertNotNull(status, "no google.rpc.Status attached");
        assertEquals(1, status.getDetailsCount(), "expected exactly one detail");
        return status.getDetails(0).unpack(ErrorInfo.class);
    }

    private static ErrorInfo errorInfoOf(Throwable cause) throws InvalidProtocolBufferException {
        return errorInfoOf(OlakeFailures.toStatusException(cause, "OP", "m"));
    }

    private static S3Exception s3Exception(String errorCode) {
        return s3Exception(errorCode, null);
    }

    private static S3Exception s3Exception(String errorCode, Throwable cause) {
        S3Exception.Builder builder = S3Exception.builder().message("denied");
        if (errorCode != null) {
            builder.awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build());
        }
        if (cause != null) {
            builder.cause(cause);
        }
        return (S3Exception) builder.build();
    }

    /** `depth` RuntimeException wrappers around an IllegalStateException. */
    private static Throwable chainOfDepth(int depth) {
        Throwable chain = new IllegalStateException("bottom");
        for (int i = 0; i < depth; i++) {
            chain = new RuntimeException("layer " + i, chain);
        }
        return chain;
    }

    // status is always INTERNAL; Go ignores the message and reads ErrorInfo instead
    @Test
    @DisplayName("status is INTERNAL and the message is unchanged")
    void statusAndMessage() {
        Status status = StatusProto.fromThrowable(
                OlakeFailures.toStatusException(new IllegalStateException("boom"), "GET_OR_CREATE_TABLE", "human text"));

        assertNotNull(status);
        assertEquals(Code.INTERNAL.getNumber(), status.getCode());
        assertEquals("human text", status.getMessage());
    }

    // a null message must not become the string "null", which Go would then treat as prose
    @Test
    @DisplayName("a null message becomes empty")
    void nullMessageIsEmpty() {
        assertEquals("", StatusProto.fromThrowable(
                OlakeFailures.toStatusException(new IllegalStateException("x"), "OP", null)).getMessage());
    }

    // Go matches on this literal, so the constant and the wire value must stay identical
    @Test
    @DisplayName("domain is olake.iceberg")
    void domainIsStamped() throws Exception {
        assertEquals("olake.iceberg", errorInfoOf(new IllegalStateException("x")).getDomain());
        assertEquals(OlakeFailures.DOMAIN, errorInfoOf(new IllegalStateException("x")).getDomain());
    }

    static Stream<Arguments> rootCauseCases() {
        return Stream.of(
                // Iceberg wraps heavily; the outermost class identifies nothing
                Arguments.of("nested wrappers report the innermost",
                        new RuntimeException("outer",
                                new IllegalStateException("middle", new NumberFormatException("innermost"))),
                        NumberFormatException.class.getName()),
                // no cause to walk
                Arguments.of("an exception with no cause is its own root",
                        new IllegalArgumentException("alone"), IllegalArgumentException.class.getName()),
                // a self-reference cannot hang the writer
                Arguments.of("a self-referencing cause terminates",
                        new SelfReferencing(), SelfReferencing.class.getName()),
                // 16 hops is the last layer that still reaches the leaf
                Arguments.of("a chain at the bound still reaches the leaf",
                        chainOfDepth(16), IllegalStateException.class.getName()),
                // past 16 hops it stops rather than walking forever
                Arguments.of("a chain deeper than the bound stops at the bound",
                        chainOfDepth(17), RuntimeException.class.getName()));
    }

    // reason is the innermost class name — that is what Go maps to a category
    @ParameterizedTest(name = "{0}")
    @MethodSource("rootCauseCases")
    @DisplayName("reason is the root cause's class")
    void reasonIsTheRootCause(String name, Throwable cause, String expectedReason) throws Exception {
        assertEquals(expectedReason, errorInfoOf(cause).getReason());
    }

    static Stream<Arguments> vendorCodeCases() {
        return Stream.of(
                // JDBC catalogs report SQLSTATE; 28000 is invalid_authorization_specification
                Arguments.of("sqlstate at the top", new SQLException("denied", "28000"), "28000"),
                // Iceberg wraps the catalog exception, so the walk must look through wrappers
                Arguments.of("sqlstate below a wrapper",
                        new RuntimeException("catalog failed", new SQLException("denied", "28000")), "28000"),
                // nested SQLSTATE: first match walking outward-in, so the outer code wins
                Arguments.of("outer sqlstate wins over inner sqlstate",
                        new SQLException("outer", "28000", new SQLException("inner", "42000")), "28000"),
                // the S3 code is the only thing separating a wrong key from a missing bucket
                Arguments.of("s3 error code", new RuntimeException("write failed", s3Exception("AccessDenied")),
                        "AccessDenied"),
                // mixed vendors: first match walking outward-in, not the innermost
                Arguments.of("outer sqlstate wins over inner s3",
                        new SQLException("denied", "28000", s3Exception("AccessDenied")), "28000"),
                Arguments.of("outer s3 wins over inner sqlstate",
                        s3Exception("AccessDenied", new SQLException("denied", "28000")), "AccessDenied"),
                // absent is omitted, not sent empty — Go treats a missing key as a real slice
                Arguments.of("no vendor cause at all", new IllegalStateException("x"), null),
                Arguments.of("sqlstate is null", new SQLException("no sqlstate"), null),
                Arguments.of("sqlstate is empty", new SQLException("no sqlstate", ""), null),
                Arguments.of("aws exception with no error details", s3Exception(null), null),
                Arguments.of("aws error code is empty", s3Exception(""), null),
                // vendorCode has its own cycle guard, separate from rootCause
                Arguments.of("self-referencing cause", new SelfReferencing(), null));
    }

    // SQLSTATE / S3 code travels when present; otherwise the key is omitted
    @ParameterizedTest(name = "{0}")
    @MethodSource("vendorCodeCases")
    @DisplayName("vendor code travels when there is one")
    void vendorCodeTravels(String name, Throwable cause, String expectedCode) throws Exception {
        Map<String, String> metadata = errorInfoOf(cause).getMetadataMap();

        if (expectedCode == null) {
            assertFalse(metadata.containsKey(OlakeFailures.METADATA_CODE),
                    "an absent code must be omitted, not sent empty");
        } else {
            assertEquals(expectedCode, metadata.get(OlakeFailures.METADATA_CODE));
        }
    }

    static Stream<Arguments> operationCases() {
        return Stream.of(
                // the call being served, e.g. GET_OR_CREATE_TABLE
                Arguments.of("supplied", "GET_OR_CREATE_TABLE", "GET_OR_CREATE_TABLE"),
                // empty/null must be omitted so Go does not read a blank operation
                Arguments.of("empty is omitted", "", null),
                Arguments.of("null is omitted", null, null));
    }

    // operation metadata is optional: sent when supplied, omitted when blank
    @ParameterizedTest(name = "{0}")
    @MethodSource("operationCases")
    @DisplayName("operation travels when supplied")
    void operationMetadata(String name, String operation, String expected) throws Exception {
        Map<String, String> metadata = errorInfoOf(
                OlakeFailures.toStatusException(new RuntimeException("x"), operation, "m")).getMetadataMap();

        if (expected == null) {
            assertFalse(metadata.containsKey(OlakeFailures.METADATA_OPERATION));
        } else {
            assertEquals(expected, metadata.get(OlakeFailures.METADATA_OPERATION));
        }
    }

    static Stream<Arguments> httpStatusCases() {
        // the AWS SDK keeps the status on its service exceptions, code or not
        S3Exception headForbidden = (S3Exception) S3Exception.builder().message("denied").statusCode(403).build();
        // OlakeRESTCatalog attaches it to what Iceberg's REST client raises
        RuntimeException restFailure = new RuntimeException("Unable to process: m");
        restFailure.addSuppressed(new OlakeFailures.HttpError(502, null));

        return Stream.of(
                Arguments.of("aws service exception", headForbidden, "403"),
                Arguments.of("aws service exception below a wrapper", new RuntimeException("x", headForbidden), "403"),
                Arguments.of("recorded by the REST client", restFailure, "502"),
                Arguments.of("recorded below a wrapper", new IllegalStateException("x", restFailure), "502"),
                // not every failure is an HTTP response
                Arguments.of("no response", new IllegalStateException("x"), null),
                Arguments.of("aws exception without a status", s3Exception("AccessDenied"), null));
    }

    // the status travels when the failure was an HTTP response; otherwise the key is omitted
    @ParameterizedTest(name = "{0}")
    @MethodSource("httpStatusCases")
    @DisplayName("http status travels when there is one")
    void httpStatusMetadata(String name, Throwable cause, String expected) throws Exception {
        Map<String, String> metadata = errorInfoOf(cause).getMetadataMap();

        if (expected == null) {
            assertFalse(metadata.containsKey(OlakeFailures.METADATA_HTTP_STATUS));
        } else {
            assertEquals(expected, metadata.get(OlakeFailures.METADATA_HTTP_STATUS));
        }
    }

    /** Built the way the AWS SDK builds its exceptions, so the builder is a frame of its own. */
    private static S3Exception builtHere() {
        return s3Exception("AccessDenied");
    }

    static Stream<Arguments> originCases() {
        // the method that created the exception, not its constructor
        Throwable thrownHere = new IllegalStateException("x");
        // the root's origin, not the wrapper's: the wrapper is created in a different method
        Throwable wrapped = new RuntimeException("outer", builtHere());
        // the JVM can drop stack traces of exceptions it throws often
        Throwable noStack = new IllegalStateException("x");
        noStack.setStackTrace(new StackTraceElement[0]);

        String test = OlakeFailuresTest.class.getName();
        return Stream.of(
                Arguments.of("raised directly", thrownHere, test + ".originCases"),
                // S3Exception$BuilderImpl.build is the top frame and must be skipped
                Arguments.of("raised through a builder", builtHere(), test + ".s3Exception"),
                Arguments.of("raised below a wrapper", wrapped, test + ".s3Exception"),
                Arguments.of("no stack trace", noStack, null));
    }

    // origin names where the root cause was raised; omitted when it cannot be known
    @ParameterizedTest(name = "{0}")
    @MethodSource("originCases")
    @DisplayName("origin travels when the stack trace has one")
    void originMetadata(String name, Throwable cause, String expected) throws Exception {
        Map<String, String> metadata = errorInfoOf(cause).getMetadataMap();

        if (expected == null) {
            assertFalse(metadata.containsKey(OlakeFailures.METADATA_ORIGIN));
        } else {
            assertEquals(expected, metadata.get(OlakeFailures.METADATA_ORIGIN));
        }
    }

    static Stream<Arguments> messageCases() {
        Throwable root = new ConnectException("Connection refused");
        return Stream.of(
                // nothing to join
                Arguments.of("a single exception sends its own message", new RuntimeException("boom"), "boom"),
                // the outermost says what was attempted, the innermost why it failed
                Arguments.of("every distinct message is joined, outermost first",
                        new RuntimeException("Failed to connect: jdbc:postgresql://h/db", new SQLException("refused", root)),
                        "Failed to connect: jdbc:postgresql://h/db <- refused <- Connection refused"),
                // new RuntimeException(cause) takes the cause's text as its own
                Arguments.of("a wrapper repeating its cause is sent once", new RuntimeException(root),
                        "java.net.ConnectException: Connection refused"),
                // absent rather than empty, like every other optional fact
                Arguments.of("no message anywhere sends none", new RuntimeException((String) null), null));
    }

    // the messages are what a person reads to see why it failed; Go masks them
    @ParameterizedTest(name = "{0}")
    @MethodSource("messageCases")
    @DisplayName("the cause chain's messages travel")
    void messageIsTheCauseChain(String name, Throwable cause, String expectedMessage) throws Exception {
        assertEquals(expectedMessage, errorInfoOf(cause).getMetadataMap().get(OlakeFailures.METADATA_MESSAGE));
    }

    // Java sends facts only (class, vendor code, where it was raised). The category is decided in Go.
    @Test
    @DisplayName("no category is sent")
    void nothingElseTravels() throws Exception {
        ErrorInfo info = errorInfoOf(OlakeFailures.toStatusException(
                new SQLException("table users column ssn is invalid", "42000"), "GET_OR_CREATE_TABLE", "m"));

        assertEquals(Map.of(
                OlakeFailures.METADATA_OPERATION, "GET_OR_CREATE_TABLE",
                OlakeFailures.METADATA_CODE, "42000",
                OlakeFailures.METADATA_MESSAGE, "table users column ssn is invalid",
                OlakeFailures.METADATA_ORIGIN, OlakeFailuresTest.class.getName() + ".nothingElseTravels"),
                info.getMetadataMap());
        assertTrue(info.getReason().startsWith("java.sql."), "reason should be a class name, got " + info.getReason());
    }

    // a failure to start has no gRPC call to ride on, so the same ErrorInfo goes to a file Go reads
    @Test
    @DisplayName("a startup failure is written as the ErrorInfo Go reads")
    void startupReport(@TempDir Path dir) throws Exception {
        RuntimeException failure = new RuntimeException("Unable to process: A warehouse 'w' does not exist");
        failure.addSuppressed(new OlakeFailures.HttpError(404, "WarehouseNotFound"));
        Path report = dir.resolve("report.pb");

        OlakeFailures.writeStartupReport(failure, report.toString());

        ErrorInfo info = ErrorInfo.parseFrom(Files.readAllBytes(report));
        assertEquals(OlakeFailures.DOMAIN, info.getDomain());
        assertEquals(RuntimeException.class.getName(), info.getReason());
        assertEquals(OlakeFailures.OPERATION_STARTUP, info.getMetadataMap().get(OlakeFailures.METADATA_OPERATION));
        assertEquals("404", info.getMetadataMap().get(OlakeFailures.METADATA_HTTP_STATUS));
        assertEquals("WarehouseNotFound", info.getMetadataMap().get(OlakeFailures.METADATA_SERVER_ERROR_TYPE));
    }

    // the writer is already failing; a report it cannot write must not replace that failure
    @Test
    @DisplayName("an unwritable report path does not throw")
    void startupReportUnwritable(@TempDir Path dir) {
        OlakeFailures.writeStartupReport(new IllegalStateException("x"),
                dir.resolve("missing-dir").resolve("report.pb").toString());
        assertFalse(Files.exists(dir.resolve("missing-dir")));
    }
}
