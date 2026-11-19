package hummingbee_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/hummingbee-ai/hummingbee-go"
)

const token = "hbk_spec_token_never_shown"

// captured is one request as the server saw it, with its body gunzipped.
type captured struct {
	Body    []byte
	Header  http.Header
	Method  string
	Path    string
	RawBody []byte
}

// recorder is a raw events endpoint: it records each request and answers with
// respond, so the client's specs need nothing from hummingbeetest.
type recorder struct {
	mu       sync.Mutex
	requests []captured
	respond  func(w http.ResponseWriter, attempt int, body []byte)
	server   *httptest.Server
}

func newRecorder(respond func(w http.ResponseWriter, attempt int, body []byte)) *recorder {
	rec := &recorder{respond: respond}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body := raw
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if body, err = io.ReadAll(zr); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		rec.mu.Lock()
		rec.requests = append(rec.requests, captured{
			Body: body, Header: r.Header.Clone(), Method: r.Method, Path: r.URL.Path, RawBody: raw,
		})
		attempt := len(rec.requests)
		rec.mu.Unlock()
		rec.respond(w, attempt, body)
	}))
	DeferCleanup(rec.server.Close)
	return rec
}

func (rec *recorder) Requests() []captured {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]captured(nil), rec.requests...)
}

// acceptAll answers every event in the request accepted, as the endpoint does.
func acceptAll(w http.ResponseWriter, _ int, body []byte) {
	var request struct {
		Events []struct {
			ID string `json:"id"`
		} `json:"events"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	results := make([]map[string]any, 0, len(request.Events))
	for i, event := range request.Events {
		results = append(results, map[string]any{"id": event.ID, "index": i, "status": "accepted"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": code, "message": "spec says " + code}})
}

func twoEvents() []hummingbee.Event {
	first := orderEvent()
	second := orderEvent()
	second.ID = "order-status-second"
	second.Changes = []hummingbee.Change{{Field: "status", From: "paid", To: "refunded"}}
	return []hummingbee.Event{first, second}
}

func manyEvents(n int) []hummingbee.Event {
	events := make([]hummingbee.Event, n)
	for i := range events {
		events[i] = orderEvent()
		events[i].ID = fmt.Sprintf("order-status-%d", i)
	}
	return events
}

var _ = Describe("Send", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("posts the batch once to /v1/events with the bearer key, gzip JSON and the user agent", func() {
		rec := newRecorder(acceptAll)
		client := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL+"/"))

		results, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(2))

		requests := rec.Requests()
		Expect(requests).To(HaveLen(1))
		request := requests[0]
		Expect(request.Method).To(Equal(http.MethodPost))
		Expect(request.Path).To(Equal("/v1/events"))
		Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + token))
		Expect(request.Header.Get("Content-Type")).To(Equal("application/json"))
		Expect(request.Header.Get("Content-Encoding")).To(Equal("gzip"))
		Expect(request.Header.Get("User-Agent")).To(HavePrefix("hummingbee-go/"))
		Expect(request.RawBody).NotTo(Equal(request.Body), "the body on the wire is compressed")

		expected, err := json.Marshal(map[string]any{"events": twoEvents()})
		Expect(err).NotTo(HaveOccurred())
		Expect(request.Body).To(MatchJSON(expected))
	})

	It("adds organization_id only when WithOrganizationID is given", func() {
		rec := newRecorder(acceptAll)
		const orgID = "0193a0b2-7c1d-7e3f-9a4b-000000000003"

		_, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL), hummingbee.WithOrganizationID(orgID)).
			Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		_, err = hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())

		requests := rec.Requests()
		Expect(requests).To(HaveLen(2))
		var with, without map[string]json.RawMessage
		Expect(json.Unmarshal(requests[0].Body, &with)).To(Succeed())
		Expect(json.Unmarshal(requests[1].Body, &without)).To(Succeed())
		Expect(with).To(HaveKeyWithValue("organization_id", json.RawMessage(`"`+orgID+`"`)))
		Expect(without).NotTo(HaveKey("organization_id"))
	})

	It("returns the server's three statuses as results in input order", func() {
		rec := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"results":[`+
				`{"id":"a","index":0,"status":"accepted"},`+
				`{"id":"b","index":1,"status":"duplicate"},`+
				`{"field":"subject.id","id":"c","index":2,"reason":"invalid_uuid","status":"rejected"}]}`)
		})
		events := manyEvents(3)

		results, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).Send(ctx, events)
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(Equal([]hummingbee.Result{
			{ID: "a", Index: 0, Status: hummingbee.StatusAccepted},
			{ID: "b", Index: 1, Status: hummingbee.StatusDuplicate},
			{Field: "subject.id", ID: "c", Index: 2, Reason: "invalid_uuid", Status: hummingbee.StatusRejected},
		}))
		Expect(results[0].IsSettled()).To(BeTrue())
		Expect(results[1].IsSettled()).To(BeTrue())
		Expect(results[2].IsSettled()).To(BeFalse())
	})

	It("refuses more than MaxBatch events before any request", func() {
		rec := newRecorder(acceptAll)

		results, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).
			Send(ctx, manyEvents(hummingbee.MaxBatch+1))
		Expect(err).To(MatchError(hummingbee.ErrTooManyEvents))
		Expect(err.Error()).To(ContainSubstring("failed to send 501 events"))
		Expect(results).To(BeNil())
		Expect(rec.Requests()).To(BeEmpty())
	})

	It("sends MaxBatch events in one request", func() {
		rec := newRecorder(acceptAll)

		results, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).
			Send(ctx, manyEvents(hummingbee.MaxBatch))
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(hummingbee.MaxBatch))
		Expect(rec.Requests()).To(HaveLen(1))
	})

	It("sends nothing for an empty batch", func() {
		rec := newRecorder(acceptAll)

		results, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).Send(ctx, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(BeNil())
		Expect(rec.Requests()).To(BeEmpty())
	})

	It("returns an *APIError for a 401 envelope after one request", func() {
		rec := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED")
		})

		_, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).Send(ctx, twoEvents())
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue())
		Expect(apiErr.StatusCode).To(Equal(http.StatusUnauthorized))
		Expect(apiErr.Code).To(Equal("UNAUTHENTICATED"))
		Expect(apiErr.Message).To(Equal("spec says UNAUTHENTICATED"))
		Expect(err.Error()).To(ContainSubstring("failed to send 2 events"))
		Expect(rec.Requests()).To(HaveLen(1))
	})

	It("returns an error naming both counts when the results do not number the events", func() {
		rec := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			writeJSON(w, http.StatusOK, map[string]any{"results": []map[string]any{{"id": "a", "index": 0, "status": "accepted"}}})
		})

		results, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL)).Send(ctx, twoEvents())
		Expect(err).To(MatchError(ContainSubstring("1 results for 2 events")))
		Expect(results).To(BeNil())
		Expect(rec.Requests()).To(HaveLen(1), "a miscounted answer is not retried")
	})

	It("returns an *APIError carrying the status when the body is not an envelope", func() {
		rec := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "<html><body>Bad gateway</body></html>")
		})

		_, err := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL), hummingbee.WithMaxAttempts(1)).
			Send(ctx, twoEvents())
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue())
		Expect(apiErr.StatusCode).To(Equal(http.StatusBadGateway))
		Expect(apiErr.Code).To(BeEmpty())
		Expect(apiErr.Message).To(Equal("Bad Gateway"))
	})

	It("never shows the token in an error or in the client's String", func() {
		unauthorized := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED")
		})
		miscounted := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			writeJSON(w, http.StatusOK, map[string]any{"results": []any{}})
		})
		closed := httptest.NewServer(http.NotFoundHandler())
		closed.Close()

		var errs []error
		for _, url := range []string{unauthorized.server.URL, miscounted.server.URL, closed.URL} {
			_, err := hummingbee.New(token, hummingbee.WithBaseURL(url), hummingbee.WithMaxAttempts(1)).Send(ctx, twoEvents())
			errs = append(errs, err)
		}
		_, err := hummingbee.New(token).Send(ctx, manyEvents(hummingbee.MaxBatch+1))
		errs = append(errs, err)

		for _, err := range errs {
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).NotTo(ContainSubstring(token))
			Expect(fmt.Sprintf("%+v", err)).NotTo(ContainSubstring(token))
		}

		client := hummingbee.New(token, hummingbee.WithBaseURL("https://events.example.test"))
		Expect(client.String()).To(Equal("hummingbee.Client(https://events.example.test)"))
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			Expect(fmt.Sprintf(format, client)).NotTo(ContainSubstring(token), format)
		}
	})
})
