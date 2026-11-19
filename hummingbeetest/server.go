package hummingbeetest

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hummingbee-ai/hummingbee-go"
)

// The limits of wire format v1.
const (
	maxBodyBytes      = 2 << 20
	maxChanges        = 100
	maxChangeValueLen = 1000
	maxFuture         = 5 * time.Minute
	maxIDLen          = 200
	maxRelated        = 50
	maxTypeLen        = 100
)

// The reasons a rejected event carries, as the real endpoint names them.
const (
	reasonInFuture           = "in_future"
	reasonInvalidActor       = "invalid_actor"
	reasonInvalidChangeValue = "invalid_change_value"
	reasonInvalidID          = "invalid_id"
	reasonInvalidType        = "invalid_type"
	reasonInvalidUUID        = "invalid_uuid"
	reasonNotAnObject        = "not_an_object"
	reasonRequired           = "required"
	reasonTooManyChanges     = "too_many_changes"
	reasonTooManyRelated     = "too_many_related"

	// Shapes wire format v1 names no reason for, which only a hand-built
	// request can send.
	reasonInvalidTime = "invalid_time"
	reasonNotAnArray  = "not_an_array"
)

var (
	eventIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	uuidPattern      = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// Server is a fake events endpoint for a firm's own tests. Start one with
// [NewServer]. Every method is safe to call from several goroutines.
type Server struct {
	mu         sync.Mutex
	events     []hummingbee.Event
	failStatus int
	failTimes  int
	now        func() time.Time
	rejections map[string]rejection
	requests   int
	seen       map[string]bool
	server     *httptest.Server
	token      string
}

type rejection struct {
	field  string
	reason string
}

// NewServer starts a fake events endpoint with a random token and its own
// memory. Close it when the test ends.
func NewServer() *Server {
	s := &Server{now: time.Now, token: newToken()}
	s.resetLocked()
	s.server = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s
}

func newToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand.Read never fails.
	return "hbk_test_" + hex.EncodeToString(b)
}

// Close shuts the server down.
func (s *Server) Close() {
	s.server.Close()
}

// URL is the server's base URL, for [hummingbee.WithBaseURL].
func (s *Server) URL() string {
	return s.server.URL
}

// Token is the only bearer key the server accepts.
func (s *Server) Token() string {
	return s.token
}

// Client returns a client already pointed at the server with its token.
// options apply after those two.
func (s *Server) Client(options ...hummingbee.Option) *hummingbee.Client {
	return hummingbee.New(s.token, append([]hummingbee.Option{hummingbee.WithBaseURL(s.URL())}, options...)...)
}

// Events returns copies of the accepted events, in arrival order.
func (s *Server) Events() []hummingbee.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]hummingbee.Event, len(s.events))
	for i, event := range s.events {
		events[i] = cloneEvent(event)
	}
	return events
}

// Requests counts every request the server answered, failed ones included.
func (s *Server) Requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// FailNext answers the next times requests with status and an error envelope,
// before any other check. The answer carries Retry-After: 0, so a client
// retries a retryable status at once.
func (s *Server) FailNext(times, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failTimes = times
	s.failStatus = status
}

// RejectID rejects every event with id from then on, with field and reason,
// before any other rule.
func (s *Server) RejectID(id, field, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejections[id] = rejection{field: field, reason: reason}
}

// Reset forgets the events, the request count, FailNext and RejectID. The
// token stays.
func (s *Server) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
}

func (s *Server) resetLocked() {
	s.events = nil
	s.failStatus = 0
	s.failTimes = 0
	s.rejections = map[string]rejection{}
	s.requests = 0
	s.seen = map[string]bool{}
}

type envelope struct {
	Events         json.RawMessage `json:"events"`
	OrganizationID json.RawMessage `json:"organization_id"`
}

type resultsResponse struct {
	Results []hummingbee.Result `json:"results"`
}

// serveHTTP holds the lock for the whole request, so a batch's duplicates and
// concurrent senders see one order.
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++

	if s.failTimes > 0 {
		s.failTimes--
		w.Header().Set("Retry-After", "0")
		writeError(w, s.failStatus, "failing as FailNext asked")
		return
	}
	if r.URL.Path != "/v1/events" {
		writeError(w, http.StatusNotFound, "only /v1/events is served")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	if !s.isAuthorized(r.Header.Get("Authorization")) {
		writeError(w, http.StatusUnauthorized, "a valid bearer key is required")
		return
	}
	if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	body, status, err := readBody(r)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	events, err := decodeEnvelope(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	results := make([]hummingbee.Result, len(events))
	var accepted []hummingbee.Event
	for i, raw := range events {
		result, event := s.judge(raw)
		result.Index = i
		results[i] = result
		if result.Status == hummingbee.StatusAccepted {
			accepted = append(accepted, event)
		}
	}
	s.events = append(s.events, accepted...)
	writeJSON(w, http.StatusOK, resultsResponse{Results: results})
}

func (s *Server) isAuthorized(header string) bool {
	scheme, key, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(key)), []byte(s.token)) == 1
}

// readBody reads the body, gunzipped when it says so, up to the wire limit
// both before and after decompressing.
func readBody(r *http.Request) ([]byte, int, error) {
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	if encoding != "" && encoding != "identity" && encoding != "gzip" {
		return nil, http.StatusUnsupportedMediaType, fmt.Errorf("Content-Encoding %q is not supported; use gzip or none", encoding)
	}
	body, err := readLimited(r.Body)
	if err != nil {
		return nil, readStatus(err), err
	}
	if encoding != "gzip" {
		return body, 0, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("the body is not gzip: %w", err)
	}
	if body, err = readLimited(zr); err != nil {
		return nil, readStatus(err), err
	}
	return body, 0, nil
}

func readStatus(err error) int {
	if errors.Is(err, errTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}

var errTooLarge = fmt.Errorf("the body is over %d bytes", maxBodyBytes)

func readLimited(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read the body: %w", err)
	}
	if len(body) > maxBodyBytes {
		return nil, errTooLarge
	}
	return body, nil
}

func decodeEnvelope(body []byte) ([]json.RawMessage, error) {
	var request envelope
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("the body is not a JSON object: %w", err)
	}
	// Absent or null means the key's own organisation.
	if !isNull(request.OrganizationID) {
		if id, ok := asString(request.OrganizationID); !ok || !uuidPattern.MatchString(id) {
			return nil, errors.New("organization_id must be a UUID")
		}
	}
	if isNull(request.Events) {
		return nil, errors.New("events is required")
	}
	var events []json.RawMessage
	if err := json.Unmarshal(request.Events, &events); err != nil {
		return nil, errors.New("events must be an array")
	}
	if len(events) > hummingbee.MaxBatch {
		return nil, fmt.Errorf("at most %d events per request, got %d", hummingbee.MaxBatch, len(events))
	}
	return events, nil
}

func isNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

func asString(raw json.RawMessage) (string, bool) {
	var s string
	return s, json.Unmarshal(raw, &s) == nil
}

// judge decides one event: rejected by RejectID or a rule, a duplicate of an
// id kept before, or accepted and remembered.
func (s *Server) judge(raw json.RawMessage) (hummingbee.Result, hummingbee.Event) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	_ = decoder.Decode(&value) // raw is an element of a decoded array, so it decodes.
	object, isObject := value.(map[string]any)
	if !isObject {
		return rejected("", "", reasonNotAnObject), hummingbee.Event{}
	}
	id, _ := object["id"].(string)

	if r, ok := s.rejections[id]; ok && id != "" {
		return rejected(id, r.field, r.reason), hummingbee.Event{}
	}
	if field, reason := checkEvent(object, s.now()); reason != "" {
		return rejected(id, field, reason), hummingbee.Event{}
	}
	if s.seen[id] {
		return hummingbee.Result{ID: id, Status: hummingbee.StatusDuplicate}, hummingbee.Event{}
	}
	var event hummingbee.Event
	if err := json.Unmarshal(raw, &event); err != nil {
		// checkEvent passed, so this is a gap in it: say so rather than accept.
		return rejected(id, "", "unexpected_shape: "+err.Error()), hummingbee.Event{}
	}
	s.seen[id] = true
	return hummingbee.Result{ID: id, Status: hummingbee.StatusAccepted}, event
}

func rejected(id, field, reason string) hummingbee.Result {
	return hummingbee.Result{Field: field, ID: id, Reason: reason, Status: hummingbee.StatusRejected}
}

// checkEvent returns the field and reason of the first rule of wire format v1
// the event breaks, or an empty reason. Missing required fields come first,
// then the rest by field, in the wire format's alphabetical order.
func checkEvent(event map[string]any, now time.Time) (field, reason string) {
	if isMissing(event["id"]) {
		return "id", reasonRequired
	}
	if isMissing(event["occurred_at"]) {
		return "occurred_at", reasonRequired
	}
	if field, reason := checkRequiredRef("subject", event["subject"]); reason != "" {
		return field, reason
	}
	if isMissing(event["type"]) {
		return "type", reasonRequired
	}
	for _, check := range []func() (string, string){
		func() (string, string) { return checkActor(event["actor"]) },
		func() (string, string) { return checkChanges(event["changes"]) },
		func() (string, string) { return checkID(event["id"]) },
		func() (string, string) { return checkOccurredAt(event["occurred_at"], now) },
		func() (string, string) { return checkRelated(event["related"]) },
		func() (string, string) { return checkRef("subject", event["subject"]) },
		func() (string, string) { return checkType(event["type"]) },
	} {
		if field, reason := check(); reason != "" {
			return field, reason
		}
	}
	return "", ""
}

func isMissing(value any) bool {
	return value == nil || value == ""
}

func checkRequiredRef(field string, value any) (string, string) {
	if value == nil {
		return field + ".id", reasonRequired
	}
	ref, ok := value.(map[string]any)
	if !ok {
		return field, reasonNotAnObject
	}
	if isMissing(ref["id"]) {
		return field + ".id", reasonRequired
	}
	if kind, _ := ref["kind"].(string); kind == "" {
		return field + ".kind", reasonRequired
	}
	return "", ""
}

func checkRef(field string, value any) (string, string) {
	if field, reason := checkRequiredRef(field, value); reason != "" {
		return field, reason
	}
	ref, _ := value.(map[string]any)
	if id, _ := ref["id"].(string); !uuidPattern.MatchString(id) {
		return field + ".id", reasonInvalidUUID
	}
	return "", ""
}

func checkActor(value any) (string, string) {
	if value == nil {
		return "", ""
	}
	actor, ok := value.(map[string]any)
	if !ok {
		return "actor", reasonNotAnObject
	}
	kind, _ := actor["kind"].(string)
	switch kind {
	case "":
		return "actor.kind", reasonRequired
	case hummingbee.ActorKindSystem:
		if name, _ := actor["name"].(string); name == "" {
			return "actor.name", reasonRequired
		}
	case hummingbee.ActorKindUser:
		if isMissing(actor["id"]) {
			return "actor.id", reasonRequired
		}
		if id, _ := actor["id"].(string); !uuidPattern.MatchString(id) {
			return "actor.id", reasonInvalidUUID
		}
	default:
		return "actor.kind", reasonInvalidActor
	}
	return "", ""
}

func checkChanges(value any) (string, string) {
	if value == nil {
		return "", ""
	}
	changes, ok := value.([]any)
	if !ok {
		return "changes", reasonNotAnArray
	}
	if len(changes) > maxChanges {
		return "changes", reasonTooManyChanges
	}
	for i, item := range changes {
		prefix := "changes[" + strconv.Itoa(i) + "]"
		change, ok := item.(map[string]any)
		if !ok {
			return prefix, reasonNotAnObject
		}
		if name, _ := change["field"].(string); name == "" {
			return prefix + ".field", reasonRequired
		}
		for _, key := range []string{"from", "to"} {
			if !isScalar(change[key]) {
				return prefix + "." + key, reasonInvalidChangeValue
			}
		}
	}
	return "", ""
}

// isScalar reports whether a change value is a string within its length, a
// number, a boolean or null.
func isScalar(value any) bool {
	switch v := value.(type) {
	case nil, bool, json.Number:
		return true
	case string:
		return utf8.RuneCountInString(v) <= maxChangeValueLen
	default:
		return false
	}
}

func checkID(value any) (string, string) {
	id, ok := value.(string)
	if !ok || len(id) > maxIDLen || !eventIDPattern.MatchString(id) {
		return "id", reasonInvalidID
	}
	return "", ""
}

func checkOccurredAt(value any, now time.Time) (string, string) {
	text, _ := value.(string)
	at, err := time.Parse(time.RFC3339, text)
	if err != nil {
		return "occurred_at", reasonInvalidTime
	}
	if at.After(now.Add(maxFuture)) {
		return "occurred_at", reasonInFuture
	}
	return "", ""
}

func checkRelated(value any) (string, string) {
	if value == nil {
		return "", ""
	}
	related, ok := value.([]any)
	if !ok {
		return "related", reasonNotAnArray
	}
	if len(related) > maxRelated {
		return "related", reasonTooManyRelated
	}
	for i, item := range related {
		if field, reason := checkRef("related["+strconv.Itoa(i)+"]", item); reason != "" {
			return field, reason
		}
	}
	return "", ""
}

func checkType(value any) (string, string) {
	eventType, ok := value.(string)
	if !ok || len(eventType) > maxTypeLen || !eventTypePattern.MatchString(eventType) {
		return "type", reasonInvalidType
	}
	return "", ""
}

func cloneEvent(event hummingbee.Event) hummingbee.Event {
	if event.Actor != nil {
		actor := *event.Actor
		event.Actor = &actor
	}
	if event.Changes != nil {
		event.Changes = append([]hummingbee.Change(nil), event.Changes...)
	}
	if event.Related != nil {
		event.Related = append([]hummingbee.Ref(nil), event.Related...)
	}
	return event
}

// errorCodes are the codes wire format v1 names. Other statuses, which only
// FailNext sends, get a code made from the status text.
var errorCodes = map[int]string{
	http.StatusBadRequest:            "BAD_REQUEST",
	http.StatusUnauthorized:          "UNAUTHENTICATED",
	http.StatusForbidden:             "FORBIDDEN",
	http.StatusRequestEntityTooLarge: "PAYLOAD_TOO_LARGE",
	http.StatusUnsupportedMediaType:  "UNSUPPORTED_MEDIA_TYPE",
	http.StatusInternalServerError:   "INTERNAL",
}

func errorCode(status int) string {
	if code, ok := errorCodes[status]; ok {
		return code
	}
	return strings.ToUpper(strings.NewReplacer(" ", "_", "-", "_", "'", "").Replace(http.StatusText(status)))
}

func writeError(w http.ResponseWriter, status int, message string) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	body := map[string]map[string]string{"error": {"code": errorCode(status), "message": message}}
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
