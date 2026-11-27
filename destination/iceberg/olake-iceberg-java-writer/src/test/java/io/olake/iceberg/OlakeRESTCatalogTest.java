package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.net.httpserver.HttpServer;

import io.olake.iceberg.rpc.OlakeFailures;

/** Loads the catalog the way OlakeRpcServer does and pins what a failed call reports. */
class OlakeRESTCatalogTest {

    private static Map<String, String> metadataOf(Throwable failure) throws Exception {
        return io.grpc.protobuf.StatusProto.fromThrowable(OlakeFailures.toStatusException(failure, "OP", "m"))
                .getDetails(0).unpack(com.google.rpc.ErrorInfo.class)
                .getMetadataMap();
    }

    /** Loads the catalog the way OlakeRpcServer does, against a server answering every call with body. */
    private static RuntimeException loadAgainst(int status, String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                exchange.getResponseBody().write(bytes);
            }
            exchange.close();
        });
        server.start();
        try {
            String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/catalog";
            return assertThrows(RuntimeException.class,
                    () -> CatalogUtil.buildIcebergCatalog("c",
                            Map.of("catalog-impl", OlakeRESTCatalog.class.getName(), "uri", uri),
                            new Configuration()));
        } finally {
            server.stop(0);
        }
    }

    static Stream<Arguments> responseCases() {
        return Stream.of(
                // the server named the error: status and type both arrive
                Arguments.of("a JSON error response", 404,
                        "{\"error\":{\"message\":\"m\",\"type\":\"WarehouseNotFound\",\"code\":404}}",
                        "WarehouseNotFound"),
                // no body to read a type from: the status alone arrives
                Arguments.of("an empty error response", 404, "", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("responseCases")
    @DisplayName("a failed catalog load reports the response it failed on")
    void reportsResponse(String name, int status, String body, String expectedType) throws Exception {
        Map<String, String> metadata = metadataOf(loadAgainst(status, body));

        assertEquals(Integer.toString(status), metadata.get(OlakeFailures.METADATA_HTTP_STATUS));
        assertEquals(expectedType, metadata.get(OlakeFailures.METADATA_SERVER_ERROR_TYPE));
    }
}
