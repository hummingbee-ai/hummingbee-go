package hummingbeetest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/hummingbee-ai/hummingbee-go"
)

// DryRun returns an HTTP client for [hummingbee.WithHTTPClient] that sends
// nothing. It logs "hummingbee dry run" once per event, with the event's id,
// type and subject kind but never its values, and answers every event
// accepted. A nil logger means [slog.Default].
func DryRun(logger *slog.Logger) *http.Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &http.Client{Transport: dryRun{logger: logger}}
}

type dryRun struct {
	logger *slog.Logger
}

// dryRunEvent is the part of an event the dry run may log.
type dryRunEvent struct {
	ID      string `json:"id"`
	Subject struct {
		Kind string `json:"kind"`
	} `json:"subject"`
	Type string `json:"type"`
}

func (d dryRun) RoundTrip(req *http.Request) (*http.Response, error) {
	events, err := readDryRunEvents(req)
	if err != nil {
		return dryRunResponse(req, http.StatusBadRequest, map[string]map[string]string{
			"error": {"code": errorCode(http.StatusBadRequest), "message": err.Error()},
		})
	}
	results := make([]hummingbee.Result, len(events))
	for i, raw := range events {
		var event dryRunEvent
		// An entry that is not an event is still answered accepted, with
		// whatever could be read of it.
		_ = json.Unmarshal(raw, &event)
		d.logger.InfoContext(req.Context(), "hummingbee dry run",
			"id", event.ID, "type", event.Type, "subject_kind", event.Subject.Kind)
		results[i] = hummingbee.Result{ID: event.ID, Index: i, Status: hummingbee.StatusAccepted}
	}
	return dryRunResponse(req, http.StatusOK, resultsResponse{Results: results})
}

func readDryRunEvents(req *http.Request) ([]json.RawMessage, error) {
	if req.Body == nil {
		return nil, fmt.Errorf("the request has no body")
	}
	defer func() { _ = req.Body.Close() }()
	var reader io.Reader = req.Body
	if strings.EqualFold(req.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(req.Body)
		if err != nil {
			return nil, fmt.Errorf("the body is not gzip: %w", err)
		}
		reader = zr
	}
	body, err := readLimited(reader)
	if err != nil {
		return nil, err
	}
	var request struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("the body is not an events envelope: %w", err)
	}
	return request.Events, nil
}

func dryRunResponse(req *http.Request, status int, value any) (*http.Response, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the dry run's answer: %w", err)
	}
	return &http.Response{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        http.Header{"Content-Type": {"application/json"}},
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Request:       req,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
	}, nil
}
