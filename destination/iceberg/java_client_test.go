package iceberg

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func serverConfigFor(t *testing.T, config *Config) map[string]string {
	t.Helper()
	raw, err := getServerConfigJSON(config, 50051, false, "")
	require.NoError(t, err)
	var props map[string]string
	require.NoError(t, json.Unmarshal(raw, &props))
	return props
}

func headerProps(props map[string]string) map[string]string {
	headers := map[string]string{}
	for k, v := range props {
		if strings.HasPrefix(k, "header.") {
			headers[k] = v
		}
	}
	return headers
}

func TestGetServerConfigJSONRestHeaders(t *testing.T) {
	t.Run("rest catalog forwards configured headers", func(t *testing.T) {
		props := serverConfigFor(t, &Config{
			CatalogType:    RestCatalog,
			RestCatalogURL: "http://localhost:8181/catalog",
			RestHeaders: map[string]string{
				"x-goog-user-project": "my-project",
				"X-Custom":            "value",
			},
		})
		require.Equal(t, map[string]string{
			"header.x-goog-user-project": "my-project",
			"header.X-Custom":            "value",
		}, headerProps(props))
	})

	t.Run("no headers when not configured", func(t *testing.T) {
		props := serverConfigFor(t, &Config{
			CatalogType:    RestCatalog,
			RestCatalogURL: "http://localhost:8181/catalog",
		})
		require.Empty(t, headerProps(props))
	})

	t.Run("dedicated header fields take precedence", func(t *testing.T) {
		props := serverConfigFor(t, &Config{
			CatalogType:          RestCatalog,
			RestCatalogURL:       "http://localhost:8181/catalog",
			GCPProjectID:         "dedicated-project",
			RESTAccessDelegation: "vended-credentials",
			RestHeaders: map[string]string{
				"x-goog-user-project":         "header-project",
				"X-Iceberg-Access-Delegation": "remote-signing",
			},
		})
		require.Equal(t, "dedicated-project", props["header.x-goog-user-project"])
		require.Equal(t, "vended-credentials", props["header.X-Iceberg-Access-Delegation"])
	})

	t.Run("ignored for non-rest catalogs", func(t *testing.T) {
		props := serverConfigFor(t, &Config{
			CatalogType: GlueCatalog,
			RestHeaders: map[string]string{"x-goog-user-project": "my-project"},
		})
		require.Empty(t, headerProps(props))
	})
}
