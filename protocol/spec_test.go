package protocol

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// secretFieldName matches spec fields that hold a secret value. Fields ending in _path hold a
// file path instead and are skipped.
var secretFieldName = regexp.MustCompile(`password|passphrase|secret|token|credential|jaas|private_key|client_key|access_key|account_key|connection_string|service_account_json`)

// Secrets are identified by "format": "password" in the connector specs, so a secret field
// without it would be sent back to clients unmasked.
func TestSpecSecretFieldsUsePasswordFormat(t *testing.T) {
	for _, pattern := range []string{"../drivers/*/resources/spec.json", "../destination/*/resources/spec.json"} {
		specFiles, err := filepath.Glob(pattern)
		require.NoError(t, err)
		require.NotEmpty(t, specFiles, pattern)

		for _, specFile := range specFiles {
			raw, err := os.ReadFile(specFile)
			require.NoError(t, err)

			var spec any
			require.NoError(t, json.Unmarshal(raw, &spec), specFile)

			unmasked := unmaskedSecrets(spec, "")
			sort.Strings(unmasked)
			assert.Empty(t, unmasked, "%s: secret fields missing \"format\": \"password\"", specFile)
		}
	}
}

// unmaskedSecrets walks every schema under node, including oneOf and dependencies branches,
// and returns the paths of secret fields that are not marked with the password format.
func unmaskedSecrets(node any, path string) []string {
	var unmasked []string
	switch n := node.(type) {
	case map[string]any:
		if properties, ok := n["properties"].(map[string]any); ok {
			for name, field := range properties {
				schema, _ := field.(map[string]any)
				if secretFieldName.MatchString(name) && !strings.HasSuffix(name, "_path") && schema["format"] != "password" {
					unmasked = append(unmasked, path+"/properties/"+name)
				}
			}
		}
		for key, child := range n {
			unmasked = append(unmasked, unmaskedSecrets(child, path+"/"+key)...)
		}
	case []any:
		for i, child := range n {
			unmasked = append(unmasked, unmaskedSecrets(child, fmt.Sprintf("%s/%d", path, i))...)
		}
	}
	return unmasked
}
