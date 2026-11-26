package iceberg

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// javaError builds the error the Iceberg writer returns over gRPC, wrapped the way callers wrap it.
func javaError(t *testing.T, reason string, metadata map[string]string) error {
	t.Helper()
	st, err := status.New(codes.Internal, "m").WithDetails(&errdetails.ErrorInfo{
		Domain:   javaErrorDomain,
		Reason:   reason,
		Metadata: metadata,
	})
	require.NoError(t, err)
	return fmt.Errorf("failed to create or get table: %w", st.Err())
}

// startupError builds the error startServer returns when the JVM exits before it is ready.
func startupError(line string) error {
	return fmt.Errorf("%w: %w", errJVMStart, errors.New("failed to start iceberg writer: exited before ready: "+line))
}

const (
	sdkClientException = "software.amazon.awssdk.core.exception.SdkClientException"
	restStartupLine    = "Iceberg writer failed to start [org.apache.iceberg.exceptions.RESTException]: Unable to process: "
)

// startupWithReport builds a failure to start whose writer left report in its report file.
func startupWithReport(t *testing.T, report *errdetails.ErrorInfo) error {
	t.Helper()
	data, err := proto.Marshal(report)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "report.pb")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return withStartupReport(startupError(restStartupLine), path)
}

func TestClassifyJavaFailure(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected errs.Failure
	}{
		// one class from several throw sites: same category, origin says which
		{
			name: "no aws credentials",
			err: javaError(t, sdkClientException, map[string]string{
				javaMetadataOrigin:    "software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain.resolveCredentials",
				javaMetadataOperation: "GET_OR_CREATE_TABLE",
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "SdkClientException",
				Origin: "AwsCredentialsProviderChain.resolveCredentials", Operation: "GET_OR_CREATE_TABLE",
			},
		},
		{
			name: "no aws region",
			err: javaError(t, sdkClientException, map[string]string{
				javaMetadataOrigin:    "software.amazon.awssdk.regions.providers.AwsRegionProviderChain.getRegion",
				javaMetadataOperation: "RECORDS",
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "SdkClientException",
				Origin: "AwsRegionProviderChain.getRegion", Operation: "RECORDS",
			},
		},
		{
			name: "no valid glue endpoint",
			err: javaError(t, sdkClientException, map[string]string{
				javaMetadataOrigin: "software.amazon.awssdk.services.glue.endpoints.internal.DefaultGlueEndpointProvider.resolveEndpoint",
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "SdkClientException",
				Origin: "DefaultGlueEndpointProvider.resolveEndpoint",
			},
		},
		// the origin only names the throw site; the class decides the category
		{
			name: "origin does not change the category",
			err: javaError(t, "java.lang.IllegalStateException", map[string]string{
				javaMetadataOrigin: "software.amazon.awssdk.regions.providers.AwsRegionProviderChain.getRegion",
			}),
			expected: errs.Failure{
				Category: errs.InternalError, ClassifiedBy: errs.ClassifiedByVendor, Code: "IllegalStateException",
				Origin: "AwsRegionProviderChain.getRegion",
			},
		},
		{
			name: "class decides when no throw site is mapped",
			err: javaError(t, "org.apache.iceberg.exceptions.ForbiddenException", map[string]string{
				javaMetadataOrigin:    "org.apache.iceberg.rest.ErrorHandlers$DefaultErrorHandler.accept",
				javaMetadataOperation: "DROP_TABLE",
			}),
			expected: errs.Failure{
				Category: errs.PermissionDenied, ClassifiedBy: errs.ClassifiedByVendor, Code: "ForbiddenException",
				Origin: "ErrorHandlers$DefaultErrorHandler.accept", Operation: "DROP_TABLE",
			},
		},
		// a status Iceberg has no class for: only the status says what the user got wrong
		{
			name: "rest status decides the category",
			err: javaError(t, "org.apache.iceberg.exceptions.RESTException", map[string]string{
				javaMetadataOrigin:     "org.apache.iceberg.rest.ErrorHandlers$DefaultErrorHandler.accept",
				javaMetadataHTTPStatus: "429",
			}),
			expected: errs.Failure{
				Category: errs.ResourceExhausted, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
				Origin: "ErrorHandlers$DefaultErrorHandler.accept", HTTPStatus: 429,
			},
		},
		// the server named a known exception: its type outranks the status
		{
			name: "rest server type names a known exception",
			err: javaError(t, "org.apache.iceberg.exceptions.RESTException", map[string]string{
				javaMetadataHTTPStatus:  "404",
				javaMetadataServerError: "NoSuchWarehouseException",
			}),
			expected: errs.Failure{
				Category: errs.ObjectNotFound, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
				HTTPStatus: 404, ServerErrorType: "NoSuchWarehouseException",
			},
		},
		// a type only that server uses falls back to the status
		{
			name: "rest server type the table does not know",
			err: javaError(t, "org.apache.iceberg.exceptions.RESTException", map[string]string{
				javaMetadataHTTPStatus:  "404",
				javaMetadataServerError: "WarehouseNotFound",
				javaMetadataMessage:     "Unable to process: A warehouse 'w' does not exist",
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
				HTTPStatus: 404, ServerErrorType: "WarehouseNotFound",
				Message: "Unable to process: A warehouse 'w' does not exist",
			},
		},
		// a status no rule names stays the catch-all's category
		{
			name: "unmapped rest status",
			err: javaError(t, "org.apache.iceberg.exceptions.RESTException", map[string]string{
				javaMetadataHTTPStatus: "409",
			}),
			expected: errs.Failure{
				Category: errs.CatalogError, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException", HTTPStatus: 409,
			},
		},
		// the status map is for the catch-all only: a class Iceberg chose outranks it
		{
			name: "rest status does not override a mapped class",
			err: javaError(t, "org.apache.iceberg.exceptions.NoSuchTableException", map[string]string{
				javaMetadataHTTPStatus: "404",
			}),
			expected: errs.Failure{
				Category: errs.ObjectNotFound, ClassifiedBy: errs.ClassifiedByVendor, Code: "NoSuchTableException", HTTPStatus: 404,
			},
		},
		// the startup path: no gRPC call, so the writer leaves the same facts in a file
		{
			name: "startup report",
			err: startupWithReport(t, &errdetails.ErrorInfo{
				Domain: javaErrorDomain,
				Reason: "org.apache.iceberg.exceptions.RESTException",
				Metadata: map[string]string{
					javaMetadataOperation:   "STARTUP",
					javaMetadataOrigin:      "org.apache.iceberg.rest.ErrorHandlers$DefaultErrorHandler.accept",
					javaMetadataHTTPStatus:  "404",
					javaMetadataServerError: "WarehouseNotFound",
				},
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
				Origin: "ErrorHandlers$DefaultErrorHandler.accept", Operation: "STARTUP", HTTPStatus: 404,
				ServerErrorType: "WarehouseNotFound",
			},
		},
		// someone else's ErrorInfo in the file is ignored; the stderr line still names the class
		{
			name: "startup report from another domain",
			err: startupWithReport(t, &errdetails.ErrorInfo{
				Domain: "other", Reason: "x.Y", Metadata: map[string]string{javaMetadataHTTPStatus: "404"},
			}),
			expected: errs.Failure{
				Category: errs.CatalogError, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
			},
		},
		// an older writer, or one killed before it wrote: the stderr line is all there is
		{
			name: "startup without a report",
			err:  withStartupReport(startupError(restStartupLine), filepath.Join(t.TempDir(), "absent.pb")),
			expected: errs.Failure{
				Category: errs.CatalogError, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
			},
		},
		{
			name: "unparsable status is dropped",
			err: javaError(t, "org.apache.iceberg.exceptions.RESTException", map[string]string{
				javaMetadataHTTPStatus: "four-oh-four",
			}),
			expected: errs.Failure{
				Category: errs.CatalogError, ClassifiedBy: errs.ClassifiedByVendor, Code: "RESTException",
			},
		},
		// the writer reports the innermost cause, so a failed handshake arrives as the cert check
		{
			name: "certificate not trusted",
			err:  javaError(t, "sun.security.provider.certpath.SunCertPathBuilderException", nil),
			expected: errs.Failure{
				Category: errs.TLSFailed, ClassifiedBy: errs.ClassifiedByVendor, Code: "SunCertPathBuilderException",
			},
		},
		{
			name: "glue signing code",
			err: javaError(t, "software.amazon.awssdk.services.glue.model.GlueException", map[string]string{
				javaMetadataCode: "InvalidSignatureException",
			}),
			expected: errs.Failure{
				Category: errs.AuthFailed, ClassifiedBy: errs.ClassifiedByVendor, Code: "InvalidSignatureException",
			},
		},
		{
			name: "sqlstate",
			err: javaError(t, "org.postgresql.util.PSQLException", map[string]string{
				javaMetadataCode: "28P01",
			}),
			expected: errs.Failure{
				Category: errs.AuthFailed, ClassifiedBy: errs.ClassifiedByVendor, Code: "28P01",
			},
		},
		// a class or code shared with unrelated failures: the throw site or the full code decides
		{
			name: "only one s3 key set",
			err: javaError(t, "org.apache.iceberg.exceptions.ValidationException", map[string]string{
				javaMetadataOrigin: "org.apache.iceberg.aws.s3.S3FileIOProperties.<init>",
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "ValidationException",
				Origin: "S3FileIOProperties.<init>",
			},
		},
		{
			name: "validation of records",
			err: javaError(t, "org.apache.iceberg.exceptions.ValidationException", map[string]string{
				javaMetadataOrigin: "org.apache.iceberg.SchemaUpdate.updateColumn",
			}),
			expected: errs.Failure{
				Category: errs.SchemaUnsupported, ClassifiedBy: errs.ClassifiedByVendor, Code: "ValidationException",
				Origin: "SchemaUpdate.updateColumn",
			},
		},
		{
			name: "no driver for the jdbc url",
			err: javaError(t, "java.sql.SQLException", map[string]string{
				javaMetadataCode:   "08001",
				javaMetadataOrigin: "java.sql.DriverManager.getConnection",
			}),
			expected: errs.Failure{
				Category: errs.ConfigInvalid, ClassifiedBy: errs.ClassifiedByVendor, Code: "08001",
				Origin: "DriverManager.getConnection",
			},
		},
		{
			name: "jdbc connection refused",
			err: javaError(t, "org.postgresql.util.PSQLException", map[string]string{
				javaMetadataCode:   "08001",
				javaMetadataOrigin: "sun.nio.ch.Net.pollConnect",
			}),
			expected: errs.Failure{
				Category: errs.NetworkUnreachable, ClassifiedBy: errs.ClassifiedByVendor, Code: "08001",
				Origin: "Net.pollConnect",
			},
		},
		{
			name: "jdbc server rejected the connection",
			err: javaError(t, "org.postgresql.util.PSQLException", map[string]string{
				javaMetadataCode: "08004",
			}),
			expected: errs.Failure{
				Category: errs.AuthFailed, ClassifiedBy: errs.ClassifiedByVendor, Code: "08004",
			},
		},
		{
			name: "jdbc insufficient privilege",
			err: javaError(t, "org.postgresql.util.PSQLException", map[string]string{
				javaMetadataCode: "42501",
			}),
			expected: errs.Failure{
				Category: errs.PermissionDenied, ClassifiedBy: errs.ClassifiedByVendor, Code: "42501",
			},
		},
		// a malformed origin is dropped, not half-reported
		{
			name: "origin without a method",
			err: javaError(t, "org.apache.iceberg.exceptions.ForbiddenException", map[string]string{
				javaMetadataOrigin: "main",
			}),
			expected: errs.Failure{
				Category: errs.PermissionDenied, ClassifiedBy: errs.ClassifiedByVendor, Code: "ForbiddenException",
			},
		},
		{
			name: "startup, port held by another writer",
			err:  startupError("Iceberg writer failed to start [java.net.BindException]: Address already in use"),
			expected: errs.Failure{
				Category: errs.ConcurrencyConflict, ClassifiedBy: errs.ClassifiedByVendor, Code: "BindException",
			},
		},
		{
			name: "startup without the line",
			err:  startupError("killed"),
			expected: errs.Failure{
				Category: errs.Unclassified, ClassifiedBy: errs.ClassifiedByDefault, Code: codeJVMStartFailed,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The path ReportFailure takes, so a failure classified where it was read counts too.
			got := errs.From(errs.Classify(tc.err))
			assert.Equal(t, component, got.Component)
			got.Component = ""
			assert.Equal(t, tc.expected, got)
		})
	}
}
