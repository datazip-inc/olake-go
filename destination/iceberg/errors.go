package iceberg

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// component names this destination in failure reports.
const component = "iceberg"

// Stamped by the writer on every ErrorInfo it attaches, so details from other libraries
// passing through are ignored.
const (
	javaErrorDomain            = "olake.iceberg"
	javaMetadataCode           = "code"
	javaMetadataOrigin         = "origin"
	javaMetadataOperation      = "operation"
	javaMetadataHTTPStatus     = "http_status"
	javaMetadataServerError    = "server_error_type"
	javaMetadataMessage        = "message"
	codeS3PathMissing          = "iceberg.s3_path_missing"
	codeJDBCURLMissing         = "iceberg.jdbc_url_missing"
	codeRestCatalogURLMissing  = "iceberg.rest_catalog_url_missing"
	codeHiveURIMissing         = "iceberg.hive_uri_missing"
	codeUnsupportedCatalogType = "iceberg.unsupported_catalog_type"
	codeJarNotFound            = "iceberg.jar_not_found"
	codeJVMStartFailed         = "iceberg.jvm_start_failed"
	codePartitionRegexNoMatch  = "iceberg.partition_regex_no_match"
)

// errJVMStart marks the startup path so the classifier can find it with errors.Is, not message text.
var errJVMStart = errors.New("failed to start iceberg java writer and setup logger")

// startupCause pulls the root cause's class out of the line OlakeRpcServer prints on its way out.
var startupCause = regexp.MustCompile(`Iceberg writer failed to start \[([^\]]+)\]`)

// startupReportPath reserves the file the writer reports a failure to start in. Empty when it
// cannot: the report only adds detail, so it must never be why the writer fails to start.
func startupReportPath() string {
	file, err := os.CreateTemp("", "olake-iceberg-startup-*.pb")
	if err != nil {
		logger.Debugf("startup failure report unavailable: %s", err)
		return ""
	}
	_ = file.Close()
	return file.Name()
}

// withStartupReport classifies a failure to start from the report the writer left at path: the
// same ErrorInfo a failed call carries, written to a file because no call exists yet. err is
// returned as is where the writer wrote none: an older writer, or one killed before it could.
func withStartupReport(err error, path string) (out error) {
	defer func() {
		if r := recover(); r != nil {
			out = err
		}
	}()

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return err
	}
	report := &errdetails.ErrorInfo{}
	if proto.Unmarshal(data, report) != nil {
		return err
	}
	f := fromErrorInfo(report)
	if f == nil {
		return err
	}
	f.Component = component
	return errs.Attach(err, *f)
}

// restStatusCategories classifies a RESTException, Iceberg's exception for a status it has no class
// of its own for, by the HTTP status the catalog answered with.
var restStatusCategories = map[int]errs.Category{
	401: errs.AuthFailed,
	403: errs.PermissionDenied,
	404: errs.ConfigInvalid, // the configured address serves nothing at that path
	405: errs.ConfigInvalid, // the configured address does not serve the Iceberg REST API
	429: errs.ResourceExhausted,
	502: errs.NetworkUnreachable,
	504: errs.Timeout,
}

// javaExceptions maps the exception the Iceberg JVM caught to a failure category.
var javaExceptions = map[string]errs.Category{
	// The catalog does not have it.
	"NoSuchTableException":        errs.ObjectNotFound,
	"NoSuchNamespaceException":    errs.ObjectNotFound,
	"NoSuchViewException":         errs.ObjectNotFound,
	"EntityNotFoundException":     errs.ObjectNotFound,
	"NoSuchKeyException":          errs.ObjectNotFound,
	"NoSuchObjectException":       errs.ObjectNotFound,
	"UnknownDBException":          errs.ObjectNotFound,
	"NotFoundException":           errs.ObjectNotFound,
	"NoSuchIcebergTableException": errs.ObjectNotFound,
	"NoSuchIcebergViewException":  errs.ObjectNotFound,

	// At the catalog or at object storage.
	"NotAuthorizedException": errs.AuthFailed,
	"ForbiddenException":     errs.PermissionDenied,
	"AccessDeniedException":  errs.PermissionDenied,
	"SaslException":          errs.AuthFailed,
	"LoginException":         errs.AuthFailed,
	"GSSException":           errs.AuthFailed,
	"AccessControlException": errs.PermissionDenied,
	"GoogleAuthException":    errs.AuthFailed,
	"TokenResponseException": errs.AuthFailed,

	// Two writers reached the same table. The first clears on retry; the others leave a commit
	// only the catalog can settle.
	"CommitFailedException":           errs.ConcurrencyConflict,
	"AlreadyExistsException":          errs.CatalogError,
	"CommitStateUnknownException":     errs.CatalogError,
	"ConcurrentModificationException": errs.ConcurrencyConflict,
	"LockException":                   errs.ConcurrencyConflict,
	"WaitingForLockException":         errs.ConcurrencyConflict,
	"VersionMismatchException":        errs.ConcurrencyConflict,

	// The records do not fit the table.
	"ValidationException": errs.SchemaUnsupported,

	// Reachability, seen from the JVM rather than from Go.
	"UnknownHostException":       errs.DNSResolutionFailed,
	"ConnectException":           errs.NetworkUnreachable,
	"NoRouteToHostException":     errs.NetworkUnreachable,
	"HttpHostConnectException":   errs.NetworkUnreachable,
	"NoHttpResponseException":    errs.NetworkUnreachable,
	"ConnectionClosedException":  errs.NetworkUnreachable,
	"SocketException":            errs.NetworkUnreachable,
	"TTransportException":        errs.NetworkUnreachable,
	"RuntimeMetaException":       errs.NetworkUnreachable,
	"SocketTimeoutException":     errs.Timeout,
	"ConnectTimeoutException":    errs.Timeout,
	"SSLHandshakeException":      errs.TLSFailed,
	"CertificateException":       errs.TLSFailed,
	"SSLPeerUnverifiedException": errs.TLSFailed,

	// TLS failures as the innermost cause reports them.
	"SSLException":                    errs.TLSFailed,
	"SunCertPathBuilderException":     errs.TLSFailed,
	"CertPathValidatorException":      errs.TLSFailed,
	"CertificateExpiredException":     errs.TLSFailed,
	"CertificateNotYetValidException": errs.TLSFailed,

	// A configured value the catalog cannot use: an address that does not parse or has no
	// scheme, or a warehouse it does not have.
	"URISyntaxException":       errs.ConfigInvalid,
	"ProtocolException":        errs.ConfigInvalid,
	"NoSuchWarehouseException": errs.ConfigInvalid,

	// The writer's port is already in use.
	"BindException": errs.ConcurrencyConflict,

	// Raised by the AWS SDK before a request is sent, for missing or conflicting client settings.
	"SdkClientException": errs.ConfigInvalid,

	// REST catalog HTTP status shapes, and leftover REST / Hive / JDBC / Glue classes.
	"ServiceUnavailableException":              errs.NetworkUnreachable,
	"ServiceFailureException":                  errs.CatalogError,
	"RESTException":                            errs.CatalogError,
	"BadRequestException":                      errs.ConfigInvalid,
	"UnprocessableEntityException":             errs.ConfigInvalid,
	"NamespaceNotEmptyException":               errs.CatalogError,
	"InvalidObjectException":                   errs.CatalogError,
	"InvalidOperationException":                errs.CatalogError,
	"TException":                               errs.CatalogError,
	"MetaException":                            errs.CatalogError, // unreachable only where overrides says so
	"TApplicationException":                    errs.CatalogError,
	"UncheckedSQLException":                    errs.CatalogError,
	"GoogleJsonResponseException":              errs.CatalogError,
	"InvalidInputException":                    errs.ConfigInvalid,
	"InternalServiceException":                 errs.NetworkUnreachable,
	"OperationTimeoutException":                errs.Timeout,
	"ResourceNumberLimitExceededException":     errs.ResourceExhausted,
	"GlueEncryptionException":                  errs.PermissionDenied,
	"UncheckedInterruptedException":            errs.Canceled,
	"SQLTimeoutException":                      errs.Timeout,
	"SQLTransientConnectionException":          errs.NetworkUnreachable,
	"SQLNonTransientConnectionException":       errs.NetworkUnreachable,
	"SQLInvalidAuthorizationSpecException":     errs.AuthFailed,
	"SQLIntegrityConstraintViolationException": errs.CatalogError,
	"SQLTransactionRollbackException":          errs.ConcurrencyConflict,
	"SQLFeatureNotSupportedException":          errs.UnsupportedFeature,

	// The JVM ran out.
	"OutOfMemoryError": errs.ResourceExhausted,

	// It understood the request and will not serve it.
	"UnsupportedOperationException": errs.UnsupportedFeature,

	// Defects in the writer, not conditions a user can fix.
	"NoClassDefFoundError":        errs.InternalError,
	"ClassNotFoundException":      errs.InternalError,
	"NullPointerException":        errs.InternalError,
	"AssertionError":              errs.InternalError,
	"IllegalArgumentException":    errs.InternalError,
	"IllegalStateException":       errs.InternalError,
	"ExceptionInInitializerError": errs.InternalError,
	"ClassCastException":          errs.InternalError,
}

// overrides maps evidence more exact than the exception class to a category, where the class is
// shared with unrelated failures: the throw site as Class.method, or the error a token endpoint
// named. Read before every other table.
var overrides = map[string]errs.Category{
	"S3FileIOProperties.<init>":                      errs.ConfigInvalid,      // ValidationException: only one of the two S3 keys is set
	"DriverManager.getConnection":                    errs.ConfigInvalid,      // 08001: no driver for the database the JDBC URL names
	"AwsCredentialsProviderChain.resolveCredentials": errs.AuthFailed,         // SdkClientException: no AWS credentials found
	"Configuration.getClassByName":                   errs.UnsupportedFeature, // ClassNotFoundException: storage the writer has no file system for
	"HiveMetaStoreClient.open":                       errs.NetworkUnreachable, // MetaException: the metastore could not be reached

	// BadRequestException, which Iceberg raises for every error a token endpoint names.
	"invalid_grant":       errs.AuthFailed,       // the credential is invalid or has expired
	"unauthorized_client": errs.PermissionDenied, // the client is known but may not use this grant
}

// sqlStates maps the SQLSTATE a JDBC catalog reports to a category, by its two-character class:
// the class is standardized across vendors, and a catalog can be any database. A full code is
// listed only where its class files it wrongly or not at all, and is read first.
var sqlStates = map[string]errs.Category{
	"28": errs.AuthFailed,          // invalid authorization specification
	"08": errs.NetworkUnreachable,  // connection exception
	"40": errs.ConcurrencyConflict, // transaction rollback
	"53": errs.ResourceExhausted,   // insufficient resources
	"57": errs.ResourceExhausted,   // operator intervention
	"3D": errs.ObjectNotFound,      // invalid catalog name
	"3F": errs.ObjectNotFound,      // invalid schema name
	"23": errs.CatalogError,        // integrity constraint violation
	"25": errs.CatalogError,        // invalid transaction state
	"0A": errs.UnsupportedFeature,  // feature not supported

	"08004": errs.AuthFailed,       // the server answered and refused the connection: no password was given
	"42501": errs.PermissionDenied, // insufficient privilege
}

// sqlStateClass returns the class of a SQLSTATE, or "" for anything that is not one.
func sqlStateClass(code string) string {
	if len(code) != 5 {
		return ""
	}
	return code[:2]
}

// grpcCategories maps a gRPC status code to a failure category — transport level only. These
// are the codes gRPC produces when the call never reached application code: the JVM is not up,
// the deadline passed, the channel was torn down. Anything the JVM caught arrives as INTERNAL.
var grpcCategories = map[codes.Code]errs.Category{
	codes.Unavailable:       errs.NetworkUnreachable, // the JVM is not listening, or the channel dropped
	codes.DeadlineExceeded:  errs.Timeout,
	codes.Canceled:          errs.Canceled,
	codes.PermissionDenied:  errs.PermissionDenied,
	codes.Unauthenticated:   errs.AuthFailed,
	codes.NotFound:          errs.ObjectNotFound,
	codes.ResourceExhausted: errs.ResourceExhausted,
	codes.Unimplemented:     errs.UnsupportedFeature,
}

// Registered so ReportFailure can classify without knowing which connector ran. Only gRPC
// evidence lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register(component, classify) }

// classify reads the gRPC status from the Iceberg JVM, then the writer marker. Order matters: a
// JVM that is not listening explains a write failure better than the write failure does.
func classify(err error) *errs.Failure {
	if f := fromGRPCStatus(err); f != nil {
		return f
	}
	if f := fromJVMStart(err); f != nil {
		return f
	}
	if destination.IsWriteFailure(err) {
		return &errs.Failure{
			Category:     errs.DestinationWriteError,
			ClassifiedBy: errs.ClassifiedByPrecondition,
		}
	}
	return nil
}

// fromJVMStart classifies a JVM that died before it could serve. Same evidence as fromErrorInfo —
// the root cause's class — arriving on stderr because no gRPC channel exists yet to carry it.
func fromJVMStart(err error) *errs.Failure {
	if !errors.Is(err, errJVMStart) {
		return nil
	}
	// Killed, or dead without naming a cause: the phase is the only thing left to report.
	f := errs.Failure{Category: errs.Unclassified, ClassifiedBy: errs.ClassifiedByDefault, Code: codeJVMStartFailed}

	match := startupCause.FindStringSubmatch(err.Error())
	if match == nil {
		return &f
	}
	// The line carries the fully-qualified class; the table is keyed on the simple name.
	f.Code = match[1]
	if i := strings.LastIndex(f.Code, "."); i >= 0 {
		f.Code = f.Code[i+1:]
	}
	if category, mapped := javaExceptions[f.Code]; mapped {
		f.Category, f.ClassifiedBy = category, errs.ClassifiedByVendor
	}
	return &f
}

// fromGRPCStatus classifies a failure the gRPC channel reported. status.FromError reaches the
// status through %w wrapping, which is why the leaf wraps on this path had to become %w.
func fromGRPCStatus(err error) *errs.Failure {
	if err == nil {
		return nil
	}
	grpcStatus, ok := status.FromError(err)
	if !ok || grpcStatus == nil || grpcStatus.Code() == codes.OK {
		return nil
	}

	// Read first: the status is INTERNAL for everything the JVM handles, so the detail is the
	// only thing that identifies which failure it was.
	for _, detail := range grpcStatus.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if f := fromErrorInfo(info); f != nil {
			return f
		}
	}

	// Prefixed because it is the transport's answer, not Iceberg's: a bare "NotFound" would
	// read as a missing table when it means the gRPC method was not found.
	code := fmt.Sprintf("grpc.%s", grpcStatus.Code().String())

	if category, mapped := grpcCategories[grpcStatus.Code()]; mapped {
		return &errs.Failure{
			Category:     category,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         code,
		}
	}

	// Almost always Internal, which the JVM sends for everything it catches and so identifies
	// nothing. Unclassified keeps that share visible; any category would be wrong for most.
	return &errs.Failure{
		Category:     errs.Unclassified,
		ClassifiedBy: errs.ClassifiedByDefault,
		Code:         code,
	}
}

// fromErrorInfo reads the structured detail the Iceberg writer attaches to a failed call. Only
// our own domain is read, so another library's ErrorInfo cannot be mistaken for one of ours.
func fromErrorInfo(info *errdetails.ErrorInfo) *errs.Failure {
	if info.GetDomain() != javaErrorDomain {
		return nil
	}

	// Reason is the fully-qualified class; the table is keyed on the simple name.
	exception := info.GetReason()
	if i := strings.LastIndex(exception, "."); i >= 0 {
		exception = exception[i+1:]
	}

	// A JDBC SQLSTATE is more precise and shares a code space with the Postgres and Db2
	// sources; otherwise the exception names the failure.
	metadata := info.GetMetadata()
	vendorCode := metadata[javaMetadataCode]
	f := &errs.Failure{
		Code:            vendorCode,
		Operation:       metadata[javaMetadataOperation],
		ServerErrorType: metadata[javaMetadataServerError],
		Message:         metadata[javaMetadataMessage],
	}
	if f.Code == "" {
		f.Code = exception
	}
	// The writer sends "package.Class.method"; Class.method is enough to name the throw site.
	if parts := strings.Split(metadata[javaMetadataOrigin], "."); len(parts) >= 2 {
		f.Origin = strings.Join(parts[len(parts)-2:], ".")
	}
	// Absent when the failure was not an HTTP response; anything unparsable is dropped the same way.
	f.HTTPStatus, _ = strconv.Atoi(metadata[javaMetadataHTTPStatus])

	if category, mapped := javaCategory(exception, vendorCode, f); mapped {
		f.Category, f.ClassifiedBy = category, errs.ClassifiedByVendor
	} else {
		// No rule covers it yet. The class name is what a rule would be written from.
		f.Category, f.ClassifiedBy, f.ErrorType = errs.Unclassified, errs.ClassifiedByDefault, info.GetReason()
	}
	return f
}

// javaCategory runs the writer's evidence through each table, most specific first. sqlState is
// the vendor code the writer sent, empty where it sent none.
func javaCategory(exception, sqlState string, f *errs.Failure) (errs.Category, bool) {
	for _, evidence := range []string{f.Origin, f.ServerErrorType} {
		if category, mapped := overrides[evidence]; mapped {
			return category, true
		}
	}
	// Iceberg's catch-all for a status it has no class for. The server's error type says what it
	// was where it names an exception the table knows; otherwise the status does.
	if exception == "RESTException" {
		if category, mapped := javaExceptions[f.ServerErrorType]; mapped {
			return category, true
		}
		if category, mapped := restStatusCategories[f.HTTPStatus]; mapped {
			return category, true
		}
	}
	if category, mapped := javaExceptions[exception]; mapped {
		return category, true
	}
	// The JVM writes straight to object storage through its own AWS SDK, and raises a bare
	// S3Exception whatever the reason — the meaning is in the service code, the same one
	// the S3 source and the parquet destination read.
	if category, mapped := objstorage.ServiceCodeCategories[f.Code]; mapped {
		return category, true
	}
	// A JDBC catalog raises classes this table cannot enumerate, but supplies a SQLSTATE
	// whose class is standardized — the same code space the SQL sources report.
	if category, mapped := sqlStates[sqlState]; mapped {
		return category, true
	}
	category, mapped := sqlStates[sqlStateClass(sqlState)]
	return category, mapped
}
