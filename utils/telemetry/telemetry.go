package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/version"
	"github.com/spf13/viper"
)

const (
	userIDFile            = "user_id"
	ipNotFoundPlaceholder = "NA"
	proxTrackURL          = "https://analytics.olake.io/mp/track"
	// flushTimeout bounds how long a failing command waits for its report to leave the process.
	flushTimeout   = 5 * time.Second
	eventPropsFile = "telemetry.json"
	distinctIDKey  = "distinct_id"
	serviceKey     = "service"
	defaultService = "CLI"
	// maxUIConfigSize bounds the read: the file is written by another process.
	maxEventPropsFileSize = 1 << 20 // 1 MiB
	// aiSkillsEnv is set to true by the OLake agent skills, so runs an agent drives can be counted.
	aiSkillsEnv = "OLAKE_AI_SKILLS"
)

// PropsFiles returns optional job-dir files S3 mode should fetch so Init can read them.
func PropsFiles() []*string {
	eventProps := eventPropsFile
	userID := userIDFile + ".txt"
	return []*string{&eventProps, &userID}
}

// Event names, qualified with the event source at send time, e.g. "Sync Started - CLI".
const (
	eventDiscover      = "Discover"
	eventSyncStarted   = "Sync Started"
	eventSyncCompleted = "Sync Completed"
	eventFailure       = "Failure"
)

type Telemetry struct {
	httpClient   *http.Client
	service      string
	platform     platformInfo
	ipAddress    string
	locationInfo *LocationInfo
	userID       string
	// aiSkills reports whether the OLake agent skills launched this run (OLAKE_AI_SKILLS).
	aiSkills bool
	// eventProps is the telemetry.json object merged onto every event last, so caller keys override the CLI defaults.
	eventProps map[string]interface{}
}

var telemetry *Telemetry

var (
	disabledOnce sync.Once
	disabled     bool

	// initDone closes once Init's background setup finishes. Init does network calls, so an
	// event sent right after start would otherwise find telemetry nil and be dropped silently.
	initDone = make(chan struct{})

	// inflight tracks handed-off events so a command about to exit can wait for them.
	inflight sync.WaitGroup
)

// Disabled reports whether telemetry is turned off via the TELEMETRY_DISABLED env var.
func Disabled() bool {
	disabledOnce.Do(func() {
		disabled, _ = strconv.ParseBool(os.Getenv("TELEMETRY_DISABLED"))
	})
	return disabled
}

type platformInfo struct {
	OS           string
	Arch         string
	OlakeVersion string
	DeviceCPU    string
}

type LocationInfo struct {
	Country string `json:"country"`
	Region  string `json:"region"`
	City    string `json:"city"`
}

func Init() {
	go func() {
		defer close(initDone)
		// check for disable
		if Disabled() {
			return
		}
		ip := getOutboundIP()
		eventProps := loadEventProps()
		telemetry = &Telemetry{
			httpClient: &http.Client{Timeout: 5 * time.Second},
			userID:     getUserID(eventProps),
			service:    getService(eventProps),
			aiSkills:   aiSkillsRun(),
			eventProps: eventProps,
			platform: platformInfo{
				OS:           runtime.GOOS,
				Arch:         runtime.GOARCH,
				OlakeVersion: version.GetOlakeCLIVersion(),
				DeviceCPU:    fmt.Sprintf("%d cores", runtime.NumCPU()),
			},
			ipAddress: ip,
		}

		if ip != ipNotFoundPlaceholder {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			loc, err := getLocationFromIP(ctx, ip)
			if err == nil {
				telemetry.locationInfo = &loc
			} else {
				logger.Debugf("Failed to fetch location for IP %s: %v", ip, err)
				telemetry.locationInfo = &LocationInfo{
					Country: "NA",
					Region:  "NA",
					City:    "NA",
				}
			}
		}
	}()
}

// send runs an event in the background, recording it so Flush can wait for it. Panics are
// contained: one here would otherwise kill the command being reported on.
func send(name string, build func()) {
	inflight.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Debugf("recovered from panic while sending %s event: %v", name, r)
			}
			inflight.Done()
		}()
		<-initDone
		if telemetry == nil {
			return
		}
		build()
	}()
}

// Flush waits for handed-off events. Commands call it before exiting:
func Flush() {
	if Disabled() {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		inflight.Wait()
	}()
	select {
	case <-done:
	case <-time.After(flushTimeout):
		logger.Debugf("telemetry flush timed out after %s", flushTimeout)
	}
}

func TrackDiscover(streamCount int, sourceType string) {
	send("discover", func() {
		props := map[string]interface{}{
			"stream_count": streamCount,
			"source_type":  sourceType,
		}
		if err := telemetry.sendEvent(eventDiscover, props); err != nil {
			logger.Debugf("Failed to send Discover event: %v", err)
		}
	})
}

// addStreamMix copies the per-sync stream breakdown onto an event. It rides on both sync
// events, which are lost independently, and seven integers are cheap.
func addStreamMix(props map[string]interface{}, mix types.StreamMix) {
	props["full_refresh_streams_count"] = mix.FullRefresh
	props["incremental_streams_count"] = mix.Incremental
	props["cdc_streams_count"] = mix.CDC
	props["strict_cdc_streams_count"] = mix.StrictCDC
	props["selected_streams_count"] = mix.Selected
	props["normalized_streams_count"] = mix.Normalized
	props["partitioned_streams_count"] = mix.Partitioned
}

// destinationShape reads the destination type and catalog type from the destination config
func destinationShape(destinationConfig *types.WriterConfig) (destinationType, catalogType string) {
	if destinationConfig == nil {
		return "", ""
	}
	destinationType = string(destinationConfig.Type)
	if destinationConfig.Type != types.Iceberg {
		return destinationType, ""
	}
	if writerConfig, ok := destinationConfig.WriterConfig.(map[string]interface{}); ok {
		catalogType, _ = writerConfig["catalog_type"].(string)
	}
	return destinationType, catalogType
}

func TrackSyncStarted(syncID string, mix types.StreamMix, sourceType string, destinationConfig *types.WriterConfig, configuredStreams int) {
	destinationType, catalogType := destinationShape(destinationConfig)

	send("sync started", func() {
		props := map[string]interface{}{
			"sync_start":                  time.Now(),
			"sync_id":                     syncID,
			"stream_count":                configuredStreams,
			"source_type":                 sourceType,
			"destination_type":            destinationType,
			"catalog_type":                catalogType,
			"stream_with_pos_update_mode": mix.StreamWithPosUpdateType,
		}

		addStreamMix(props, mix)

		if err := telemetry.sendEvent(eventSyncStarted, props); err != nil {
			logger.Debugf("Failed to send SyncStarted event: %v", err)
		}
	})
}

func TrackSyncCompleted(syncID string, mix types.StreamMix, sourceType string, destinationConfig *types.WriterConfig, status bool, records, bytesRead int64) {
	destinationType, catalogType := destinationShape(destinationConfig)

	send("sync completed", func() {
		props := map[string]interface{}{
			"sync_id":          syncID,
			"sync_end":         time.Now(),
			"sync_status":      utils.Ternary(status, "SUCCESS", "FAILED").(string),
			"records_synced":   records,
			"bytes_read":       bytesRead,
			"source_type":      sourceType,
			"destination_type": destinationType,
			"catalog_type":     catalogType,
		}
		addStreamMix(props, mix)

		if err := telemetry.sendEvent(eventSyncCompleted, props); err != nil {
			logger.Debugf("Failed to send SyncCompleted event: %v", err)
		}
	})
}

// What replaces sensitive text in a failure message.
const (
	urlMask   = "<URL>"
	hostMask  = "<HOST>"
	valueMask = "xxx"
	// passwordMessage replaces a whole message that mentions a password.
	passwordMessage = "error most likely due to wrong password"
)

// Mixpanel cuts a string property at 255 characters, so the message is spread over a few.
const (
	messagePropSize  = 255
	messagePropCount = 4
)

// The writer config fields whose values must not appear in a failure message.
var (
	addressKeys    = []string{"jdbc_url", "rest_catalog_url", "oauth2_uri", "hive_uri", "s3_endpoint", "glue_endpoint", "iceberg_s3_path"}
	credentialKeys = []string{"jdbc_username", "jdbc_password"}
)

// address is a URL of any scheme: a catalog, a storage path or a JDBC address, which can carry a
// user and password. Also what follows a configured value already replaced, so a path under the
// configured storage path goes with it.
var address = regexp.MustCompile(`(?:jdbc:|[A-Za-z][\w+.-]*://)\S*|` + urlMask + `/\S*`)

// hostIn finds the host in one address: after the scheme and any user:password@, up to the port
// or path.
var hostIn = regexp.MustCompile(`^(?:.*://)?(?:[^/]*@)?([^:/?]+)`)

// shapeMasks are the forms sensitive text takes that no config value matches, each with what
// replaces it. "${1}" keeps the words that introduce the value.
var shapeMasks = []struct {
	shape *regexp.Regexp
	mask  string
}{
	// A row value, where the Java writer and Iceberg's Parquet writer quote one they could not
	// write. Everything after it is the value or an error repeating it.
	{regexp.MustCompile(`(?s)(for field .*? value |raw value: |Cannot write value[^:]*: ).*`), "${1}" + valueMask},
	// The address a host resolved to, as Java prints it.
	{regexp.MustCompile(`\b(?:\d{1,3}(?:\.\d{1,3}){3}|(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4})\b`), hostMask},
	// The user or role Postgres rejected.
	{regexp.MustCompile(`((?:user|role) )"[^"]*"`), "${1}" + valueMask},
	// An AWS resource name, which holds the account and, in a denial, the user.
	{regexp.MustCompile(`arn:aws[\w-]*:[^\s,)]+`), valueMask},
}

// maskMessage removes addresses, hosts, storage paths, user names, passwords and record values
// from a failure message, and replaces a message that mentions a password altogether.
func maskMessage(message string, destinationConfig *types.WriterConfig) string {
	var writerConfig map[string]interface{}
	if destinationConfig != nil {
		writerConfig, _ = destinationConfig.WriterConfig.(map[string]interface{})
	}

	// 1. What the config holds, exactly as written.
	for _, key := range addressKeys {
		if value, _ := writerConfig[key].(string); value != "" {
			message = strings.ReplaceAll(message, value, urlMask)
		}
	}
	for _, key := range credentialKeys {
		if value, _ := writerConfig[key].(string); value != "" {
			message = strings.ReplaceAll(message, value, valueMask)
		}
	}

	// 2. Any other address. Before the password check, so a URL carrying "password=" does not
	// turn every failure into a password one.
	message = address.ReplaceAllString(message, urlMask)
	if strings.Contains(strings.ToLower(message), "password") {
		return passwordMessage
	}

	// 3. A configured host named alone, with whatever is attached to it: a port, a resolved
	// address. A value can list several hosts.
	for _, key := range addressKeys {
		value, _ := writerConfig[key].(string)
		for _, one := range strings.Split(value, ",") {
			if host := hostIn.FindStringSubmatch(one); host != nil {
				attached := regexp.MustCompile(`\S*` + regexp.QuoteMeta(host[1]) + `\S*`)
				message = attached.ReplaceAllString(message, hostMask)
			}
		}
	}

	// 4. The shapes no config value matches.
	for _, m := range shapeMasks {
		message = m.shape.ReplaceAllString(message, m.mask)
	}
	return strings.Join(strings.Fields(message), " ")
}

// TrackFailure reports why a command failed. Every field but the message is a constant from this
// repo or a code the vendor defines. The message is the error text the Iceberg writer raised,
// masked by maskMessage. Absent fields are normal.
func TrackFailure(command, errorSource, syncID string, destinationConfig *types.WriterConfig, f errs.Failure) {
	_, catalogType := destinationShape(destinationConfig)

	send("failure - cli", func() {
		// The rest of the run's shape is on the sync events, reachable through sync_id.
		props := map[string]interface{}{
			"command":       command,
			"error_source":  errorSource,
			"category":      string(f.Category),
			"classified_by": f.ClassifiedBy,
		}
		// Absent for every command except sync.
		if syncID != "" {
			props["sync_id"] = syncID
		}
		// Only send what exists: an absent code is a legitimate slice, not a value to fake.
		if f.Code != "" {
			props["code"] = f.Code
		}
		if f.ErrorType != "" {
			props["error_type"] = f.ErrorType
		}
		// Set only when an Iceberg destination config was read.
		if catalogType != "" {
			props["catalog_type"] = catalogType
		}
		// Set only by the Iceberg writer.
		if f.Origin != "" {
			props["origin"] = f.Origin
		}
		if f.Operation != "" {
			props["operation"] = f.Operation
		}
		if f.HTTPStatus != 0 {
			props["http_status"] = f.HTTPStatus
		}
		if f.ServerErrorType != "" {
			props["server_error_type"] = f.ServerErrorType
		}
		// error_message, then error_message_2 and so on while text remains.
		message := maskMessage(f.Message, destinationConfig)
		for part := 1; part <= messagePropCount && message != ""; part++ {
			key := "error_message"
			if part > 1 {
				key = fmt.Sprintf("error_message_%d", part)
			}
			size := min(len(message), messagePropSize)
			props[key], message = message[:size], message[size:]
		}

		if err := telemetry.sendEvent(eventFailure, props); err != nil {
			logger.Debugf("Failed to send Failure event: %v", err)
		}
	})
}

func (t *Telemetry) sendEvent(event string, props map[string]interface{}) error {
	if t.httpClient == nil {
		return fmt.Errorf("telemetry client is nil")
	}

	eventName := fmt.Sprintf("%s - %s", event, t.service)

	// Add common properties
	if props == nil {
		props = make(map[string]interface{})
	}
	properties := map[string]interface{}{
		"os":                  t.platform.OS,
		"arch":                t.platform.Arch,
		"olake_version":       t.platform.OlakeVersion,
		"num_cpu":             t.platform.DeviceCPU,
		"service":             t.service,
		"ip_address":          t.ipAddress,
		"location":            t.locationInfo,
		"distinct_id":         t.userID,
		"time":                time.Now().Unix(),
		"event_original_name": eventName,
		"ai_skills":           t.aiSkills,
	}

	for key, value := range properties {
		props[key] = value
	}

	// Applied last so the event props can override the default props.
	for key, value := range t.eventProps {
		props[key] = value
	}

	body := map[string]interface{}{
		"event":      eventName,
		"properties": props,
	}
	propsBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", proxTrackURL, strings.NewReader(string(propsBody)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send telemetry event, status: %s, response: %s", resp.Status, string(respBody))
	}
	return nil
}

func getOutboundIP() string {
	ip := []byte(ipNotFoundPlaceholder)
	// Timeout to the client so that init dosen't hang forever
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("https://api.ipify.org?format=text")

	if err != nil {
		return string(ip)
	}

	defer resp.Body.Close()
	ipBody, err := io.ReadAll(resp.Body)
	if err == nil {
		ip = ipBody
	}

	return string(ip)
}

// aiSkillsRun reports whether OLAKE_AI_SKILLS is true; unset or unparsable is false.
func aiSkillsRun() bool {
	aiSkills, _ := strconv.ParseBool(os.Getenv(aiSkillsEnv))
	return aiSkills
}

// getService returns telemetry.json's service, defaulting to the CLI.
func getService(eventProps map[string]interface{}) string {
	if service, ok := eventProps[serviceKey].(string); ok {
		if service = strings.TrimSpace(service); service != "" {
			return service
		}
	}
	return defaultService
}

// loadEventProps reads telemetry.json; an absent or malformed one is dropped, never failing the run.
func loadEventProps() map[string]interface{} {
	configFolder := viper.GetString(constants.ConfigFolder)
	if configFolder == "" {
		return nil
	}

	file, err := os.Open(filepath.Join(configFolder, eventPropsFile))
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Debugf("Failed to open %s: %v", eventPropsFile, err)
		}
		return nil
	}
	defer file.Close()

	var props map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(file, maxEventPropsFileSize)).Decode(&props); err != nil {
		logger.Debugf("Ignoring %s, not a JSON object: %v", eventPropsFile, err)
		return nil
	}
	return props
}

// getUserID prefers telemetry.json's distinct_id; older callers still write user_id.txt.
func getUserID(eventProps map[string]interface{}) string {
	if id, ok := eventProps[distinctIDKey].(string); ok {
		if id = strings.TrimSpace(id); id != "" {
			return id
		}
	}

	// check if id file exists
	configFolder := viper.GetString(constants.ConfigFolder)
	if configFolder != "" {
		if idBytes, err := os.ReadFile(filepath.Join(configFolder, fmt.Sprintf("%s.txt", userIDFile))); err == nil {
			uID := strings.Trim(string(idBytes), `"`)
			return uID
		}
	}

	// Generate new ID
	hash := sha256.New()
	hash.Write([]byte(time.Now().String()))
	generatedID := hex.EncodeToString(hash.Sum(nil))[:32]
	_ = logger.FileLogger(generatedID, userIDFile, ".txt")
	return generatedID
}

func getLocationFromIP(ctx context.Context, ip string) (LocationInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://ipinfo.io/%s/json", ip), nil)
	if err != nil {
		return LocationInfo{}, err
	}

	client := http.Client{Timeout: 1 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return LocationInfo{}, err
	}
	defer resp.Body.Close()

	var info struct {
		Country string `json:"country"`
		Region  string `json:"region"`
		City    string `json:"city"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return LocationInfo{}, err
	}

	return LocationInfo{
		Country: info.Country,
		Region:  info.Region,
		City:    info.City,
	}, nil
}
