package io.olake.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.ErrorRecordingClient;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Iceberg's REST catalog, built on the client RESTCatalog builds by default wrapped in an
 * ErrorRecordingClient, so a failed call reports the server's HTTP response as well as the
 * exception Iceberg raises for it.
 */
public class OlakeRESTCatalog extends RESTCatalog {

    public OlakeRESTCatalog() {
        super(SessionCatalog.SessionContext.createEmpty(),
                config -> new ErrorRecordingClient(HTTPClient.builder(config)
                        .uri(config.get(CatalogProperties.URI))
                        .withHeaders(RESTUtil.configHeaders(config))
                        .build()));
    }
}
