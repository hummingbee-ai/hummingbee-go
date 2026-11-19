package hummingbee

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// MaxBatch is the most events one [Client.Send] takes.
const MaxBatch = 500

const (
	eventsPath = "/v1/events"
	// Far above 500 results, so a sane answer is never cut short.
	maxResponseBytes = 4 << 20
	maxErrorBytes    = 64 << 10
)

// Status is what the endpoint did with one event.
type Status string

// The statuses an event can have.
const (
	// StatusAccepted means Hummingbee kept the event.
	StatusAccepted Status = "accepted"
	// StatusDuplicate means Hummingbee already had an event with this id and
	// kept the first copy.
	StatusDuplicate Status = "duplicate"
	// StatusRejected means the event broke a rule of the wire format. Field and
	// Reason say which. Sending it again is rejected again.
	StatusRejected Status = "rejected"
)

// Result is the endpoint's answer for one event.
type Result struct {
	// Field is the field that broke a rule, such as "subject.id". Only on
	// rejected events.
	Field string `json:"field,omitempty"`
	// ID is the event's id.
	ID string `json:"id"`
	// Index is the event's position in the batch, from zero.
	Index int `json:"index"`
	// Reason names the rule, such as "invalid_uuid". Only on rejected events.
	Reason string `json:"reason,omitempty"`
	// Status is what the endpoint did with the event.
	Status Status `json:"status"`
}

// IsSettled reports whether the event never needs sending again: Hummingbee
// has it, from this request or an earlier one.
func (r Result) IsSettled() bool {
	return r.Status == StatusAccepted || r.Status == StatusDuplicate
}

type sendRequest struct {
	Events         []Event `json:"events"`
	OrganizationID string  `json:"organization_id,omitempty"`
}

type sendResponse struct {
	Results []Result `json:"results"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Send posts events in one request and returns one [Result] per event, in the
// order given.
//
// More than [MaxBatch] events return [ErrTooManyEvents] before any request,
// and no events send nothing. Transport errors and the statuses 408, 429, 500,
// 502, 503 and 504 are retried with backoff, honouring Retry-After, up to the
// attempt limit and never past ctx's deadline. Every other response that is
// not a success returns at once as an [*APIError], and so does the last
// attempt's retryable status.
func (c *Client) Send(ctx context.Context, events []Event) ([]Result, error) {
	if len(events) > MaxBatch {
		return nil, fmt.Errorf("failed to send %d events: %w", len(events), ErrTooManyEvents)
	}
	if len(events) == 0 {
		return nil, nil
	}
	body, err := c.encode(events)
	if err != nil {
		return nil, fmt.Errorf("failed to send %d events: %w", len(events), err)
	}
	results, err := c.sendWithRetries(ctx, body, len(events))
	if err != nil {
		return nil, fmt.Errorf("failed to send %d events: %w", len(events), err)
	}
	return results, nil
}

// encode gzips the request once, so every attempt sends the same bytes.
func (c *Client) encode(events []Event) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(sendRequest{Events: events, OrganizationID: c.organizationID}); err != nil {
		return nil, fmt.Errorf("failed to encode events: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("failed to compress events: %w", err)
	}
	return buf.Bytes(), nil
}

// attempt posts body once and reads the answer.
func (c *Client) attempt(ctx context.Context, body []byte, count int) ([]Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+eventsPath, bytes.NewReader(body))
	if err != nil {
		return nil, &permanentError{err: fmt.Errorf("failed to build request: %w", err)}
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drained so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, readAPIError(resp)
	}
	// A body that breaks off is a transport error and is retried; one read in
	// full that does not decode is permanent.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to read results: %w", err)
	}
	var decoded sendResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, &permanentError{err: fmt.Errorf("failed to decode results: %w", err)}
	}
	if len(decoded.Results) != count {
		return nil, &permanentError{err: fmt.Errorf("server returned %d results for %d events", len(decoded.Results), count)}
	}
	return decoded.Results, nil
}

func readAPIError(resp *http.Response) *APIError {
	apiErr := &APIError{
		Message:    http.StatusText(resp.StatusCode),
		StatusCode: resp.StatusCode,
		retryAfter: resp.Header.Get("Retry-After"),
	}
	var envelope errorResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxErrorBytes)).Decode(&envelope); err == nil && envelope.Error.Code != "" {
		apiErr.Code = envelope.Error.Code
		apiErr.Message = envelope.Error.Message
	}
	return apiErr
}
