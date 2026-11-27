package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;

import io.olake.iceberg.rpc.OlakeFailures;

/**
 * Iceberg's HTTP client, with each error response recorded on the exception it raises: the HTTP
 * status and the error type the server named, which Iceberg's REST exceptions do not keep.
 *
 * <p>In Iceberg's package because every call goes through BaseHTTPClient's protected execute,
 * the one place an error handler can be wrapped.
 */
public class ErrorRecordingClient extends BaseHTTPClient {
    private final BaseHTTPClient client;

    public ErrorRecordingClient(BaseHTTPClient client) {
        this.client = client;
    }

    @Override
    public RESTClient withAuthSession(AuthSession session) {
        return new ErrorRecordingClient((BaseHTTPClient) client.withAuthSession(session));
    }

    @Override
    protected HTTPRequest buildRequest(HTTPMethod method, String path, Map<String, String> queryParams,
            Map<String, String> headers, Object body) {
        return client.buildRequest(method, path, queryParams, headers, body);
    }

    @Override
    protected <T extends RESTResponse> T execute(HTTPRequest request, Class<T> responseType,
            Consumer<ErrorResponse> errorHandler, Consumer<Map<String, String>> responseHeaders) {
        return client.execute(request, responseType, new Recorder(errorHandler), responseHeaders);
    }

    @Override
    protected <T extends RESTResponse> T execute(HTTPRequest request, Class<T> responseType,
            Consumer<ErrorResponse> errorHandler, Consumer<Map<String, String>> responseHeaders,
            ParserContext parserContext) {
        return client.execute(request, responseType, new Recorder(errorHandler), responseHeaders, parserContext);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /** Lets the wrapped handler decide as before, and attaches the response to what it throws. */
    private static final class Recorder extends ErrorHandler {
        private final Consumer<ErrorResponse> handler;

        Recorder(Consumer<ErrorResponse> handler) {
            this.handler = handler;
        }

        @Override
        public ErrorResponse parseResponse(int code, String json) {
            if (handler instanceof ErrorHandler) {
                return ((ErrorHandler) handler).parseResponse(code, json);
            }
            // What HTTPClient builds for a handler that cannot parse.
            return ErrorResponse.builder().responseCode(code).withMessage(json).build();
        }

        @Override
        public void accept(ErrorResponse error) {
            try {
                handler.accept(error);
            } catch (RuntimeException e) {
                e.addSuppressed(new OlakeFailures.HttpError(error.code(), error.type()));
                throw e;
            }
        }
    }
}
