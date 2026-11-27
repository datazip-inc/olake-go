package org.apache.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.rpc.ErrorInfo;

import io.grpc.protobuf.StatusProto;
import io.olake.iceberg.rpc.OlakeFailures;

/** Pins that every call through the client reports the error response it failed on. */
class ErrorRecordingClientTest {

    /** Answers every call with the same error response, the way HTTPClient hands one to the handler. */
    private static final class FailingClient extends BaseHTTPClient {
        private final int status;
        private final String type;

        FailingClient(int status, String type) {
            this.status = status;
            this.type = type;
        }

        @Override
        public RESTClient withAuthSession(AuthSession session) {
            return new FailingClient(status, type);
        }

        @Override
        protected HTTPRequest buildRequest(HTTPMethod method, String path, Map<String, String> queryParams,
                Map<String, String> headers, Object body) {
            return null;
        }

        @Override
        protected <T extends RESTResponse> T execute(HTTPRequest request, Class<T> responseType,
                Consumer<ErrorResponse> errorHandler, Consumer<Map<String, String>> responseHeaders) {
            errorHandler.accept(ErrorResponse.builder().responseCode(status).withType(type).withMessage("m").build());
            throw new AssertionError("the handler should have thrown");
        }

        @Override
        public void close() {
        }
    }

    /** One call made through a client. */
    private interface Call {
        void on(RESTClient client);
    }

    private static Map<String, String> metadataOf(Throwable failure) throws Exception {
        return StatusProto.fromThrowable(OlakeFailures.toStatusException(failure, "OP", "m"))
                .getDetails(0).unpack(ErrorInfo.class).getMetadataMap();
    }

    static Stream<Arguments> callCases() {
        Consumer<ErrorResponse> catchAll = ErrorHandlers.defaultErrorHandler();
        return Stream.of(
                // a status Iceberg has no class for raises its catch-all; status and type still arrive
                Arguments.of("get, with a server error type", (Call) c -> c.get("v1/config", Map.of(),
                        LoadTableResponse.class, Map.of(), catchAll), 429, "TooManyRequests", RESTException.class),
                // every public call goes through execute, overloads included
                Arguments.of("post", (Call) c -> c.post("p", null, LoadTableResponse.class, Map.of(), catchAll),
                        502, null, RESTException.class),
                Arguments.of("delete", (Call) c -> c.delete("p", LoadTableResponse.class, Map.of(), catchAll),
                        502, null, RESTException.class),
                Arguments.of("form post", (Call) c -> c.postForm("p", Map.of(), LoadTableResponse.class, Map.of(),
                        catchAll), 502, null, RESTException.class),
                Arguments.of("get through an overload", (Call) c -> c.get("p", LoadTableResponse.class, Map.of(),
                        catchAll), 502, null, RESTException.class),
                // the handler decides the exception as before; the response only rides along
                Arguments.of("head, with a status Iceberg maps", (Call) c -> c.head("v1/namespaces/db/tables/t",
                        Map.of(), ErrorHandlers.tableErrorHandler()), 404, null, NoSuchTableException.class));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("callCases")
    @DisplayName("a failed call reports the status and server error type it failed on")
    void reportsResponse(String name, Call call, int status, String type, Class<? extends RuntimeException> expected)
            throws Exception {
        try (RESTClient client = new ErrorRecordingClient(new FailingClient(status, type))) {
            Map<String, String> metadata = metadataOf(assertThrows(expected, () -> call.on(client)));

            assertEquals(Integer.toString(status), metadata.get(OlakeFailures.METADATA_HTTP_STATUS));
            assertEquals(type, metadata.get(OlakeFailures.METADATA_SERVER_ERROR_TYPE));
        }
    }

    // the client handed out after authentication records as well
    @Test
    @DisplayName("the authenticated client reports the response")
    void authenticated() throws Exception {
        try (RESTClient root = new ErrorRecordingClient(new FailingClient(401, null));
                RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            RESTException e = assertThrows(RESTException.class,
                    () -> client.post("p", null, LoadTableResponse.class, Map.of(), ErrorHandlers.oauthErrorHandler()));
            assertEquals("401", metadataOf(e).get(OlakeFailures.METADATA_HTTP_STATUS));
        }
    }

    // the message the operator reads is unchanged
    @Test
    @DisplayName("the exception message is unchanged")
    void messageUnchanged() throws Exception {
        try (RESTClient client = new ErrorRecordingClient(new FailingClient(404, null))) {
            RESTException e = assertThrows(RESTException.class,
                    () -> client.get("v1/config", Map.of(), LoadTableResponse.class, Map.of(),
                            ErrorHandlers.defaultErrorHandler()));
            assertEquals("Unable to process: m", e.getMessage());
        }
    }
}
