package hummingbeetest_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/hummingbee-ai/hummingbee-go"
	"github.com/hummingbee-ai/hummingbee-go/hummingbeetest"
)

const (
	customerID = "0193a0b2-7c1d-7e3f-9a4b-000000000001"
	orderID    = "0193a0b2-7c1d-7e3f-9a4b-000000000002"
)

func orderEvent(id string) hummingbee.Event {
	return hummingbee.Event{
		Actor:      hummingbee.System("order-sync"),
		Changes:    []hummingbee.Change{{Field: "status", From: "submitted", To: "paid"}},
		ID:         id,
		OccurredAt: time.Now().UTC().Truncate(time.Second),
		Related:    []hummingbee.Ref{{ID: customerID, Kind: "customer"}},
		Subject:    hummingbee.Ref{ID: orderID, Kind: "order"},
		Type:       "order.status_changed",
	}
}

type rawResponse struct {
	Body   []byte
	Header http.Header
	Status int
}

// post sends body as it is, so specs can break what the client never would.
func post(server *hummingbeetest.Server, header map[string]string, body []byte) rawResponse {
	GinkgoHelper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL()+"/v1/events", bytes.NewReader(body))
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Authorization", "Bearer "+server.Token())
	req.Header.Set("Content-Type", "application/json")
	for key, value := range header {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return rawResponse{Body: b, Header: resp.Header, Status: resp.StatusCode}
}

func envelopeOf(events ...any) []byte {
	GinkgoHelper()
	b, err := json.Marshal(map[string]any{"events": events})
	Expect(err).NotTo(HaveOccurred())
	return b
}

func errorCode(resp rawResponse) string {
	GinkgoHelper()
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	Expect(json.Unmarshal(resp.Body, &body)).To(Succeed(), string(resp.Body))
	Expect(body.Error.Message).NotTo(BeEmpty())
	return body.Error.Code
}

func resultsOf(resp rawResponse) []hummingbee.Result {
	GinkgoHelper()
	Expect(resp.Status).To(Equal(http.StatusOK), string(resp.Body))
	var body struct {
		Results []hummingbee.Result `json:"results"`
	}
	Expect(json.Unmarshal(resp.Body, &body)).To(Succeed())
	return body.Results
}

var _ = Describe("Server", func() {
	var (
		ctx    context.Context
		server *hummingbeetest.Server
	)

	BeforeEach(func() {
		ctx = context.Background()
		server = hummingbeetest.NewServer()
		DeferCleanup(server.Close)
	})

	It("records what Client sends, accepting both events (covers the done-when)", func() {
		events := []hummingbee.Event{orderEvent("order-status-1"), orderEvent("order-status-2")}

		results, err := server.Client().Send(ctx, events)
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(Equal([]hummingbee.Result{
			{ID: "order-status-1", Index: 0, Status: hummingbee.StatusAccepted},
			{ID: "order-status-2", Index: 1, Status: hummingbee.StatusAccepted},
		}))
		Expect(server.Events()).To(Equal(events))
		Expect(server.Requests()).To(Equal(1))
	})

	It("reports a re-sent event duplicate and keeps one copy (covers the done-when)", func() {
		client := server.Client()
		event := orderEvent("order-status-1")
		_, err := client.Send(ctx, []hummingbee.Event{event})
		Expect(err).NotTo(HaveOccurred())

		event.Changes = []hummingbee.Change{{Field: "status", From: "paid", To: "refunded"}}
		results, err := client.Send(ctx, []hummingbee.Event{event})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(Equal([]hummingbee.Result{{ID: "order-status-1", Status: hummingbee.StatusDuplicate}}))

		Expect(server.Events()).To(HaveLen(1))
		Expect(server.Events()[0].Changes[0].To).To(Equal("paid"), "the first copy is kept")
	})

	It("accepts the first of two copies in one batch and reports the second duplicate", func() {
		event := orderEvent("order-status-1")

		results, err := server.Client().Send(ctx, []hummingbee.Event{event, event})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(Equal([]hummingbee.Result{
			{ID: "order-status-1", Index: 0, Status: hummingbee.StatusAccepted},
			{ID: "order-status-1", Index: 1, Status: hummingbee.StatusDuplicate},
		}))
		Expect(server.Events()).To(HaveLen(1))
	})

	It("lets the client retry a 503 from FailNext and succeed (covers the done-when)", func() {
		server.FailNext(1, http.StatusServiceUnavailable)

		results, err := server.Client().Send(ctx, []hummingbee.Event{orderEvent("order-status-1")})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Status).To(Equal(hummingbee.StatusAccepted))
		Expect(server.Requests()).To(Equal(2))
		Expect(server.Events()).To(HaveLen(1))
	})

	It("answers FailNext's status with an envelope the client reads", func() {
		server.FailNext(1, http.StatusForbidden)

		_, err := server.Client().Send(ctx, []hummingbee.Event{orderEvent("order-status-1")})
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue())
		Expect(apiErr.StatusCode).To(Equal(http.StatusForbidden))
		Expect(apiErr.Code).To(Equal("FORBIDDEN"))
		Expect(server.Requests()).To(Equal(1))
	})

	Describe("a request as a whole", func() {
		It("answers a wrong token 401 with WWW-Authenticate: Bearer", func() {
			resp := post(server, map[string]string{"Authorization": "Bearer hbk_wrong"}, envelopeOf(orderEvent("a")))
			Expect(resp.Status).To(Equal(http.StatusUnauthorized))
			Expect(resp.Header.Get("WWW-Authenticate")).To(Equal("Bearer"))
			Expect(errorCode(resp)).To(Equal("UNAUTHENTICATED"))
		})

		It("answers no Authorization header 401", func() {
			resp := post(server, map[string]string{"Authorization": ""}, envelopeOf(orderEvent("a")))
			Expect(resp.Status).To(Equal(http.StatusUnauthorized))
			Expect(errorCode(resp)).To(Equal("UNAUTHENTICATED"))
		})

		It("answers a text/plain body 415", func() {
			resp := post(server, map[string]string{"Content-Type": "text/plain"}, envelopeOf(orderEvent("a")))
			Expect(resp.Status).To(Equal(http.StatusUnsupportedMediaType))
			Expect(errorCode(resp)).To(Equal("UNSUPPORTED_MEDIA_TYPE"))
		})

		It("answers Content-Encoding: br 415", func() {
			resp := post(server, map[string]string{"Content-Encoding": "br"}, envelopeOf(orderEvent("a")))
			Expect(resp.Status).To(Equal(http.StatusUnsupportedMediaType))
			Expect(errorCode(resp)).To(Equal("UNSUPPORTED_MEDIA_TYPE"))
		})

		It("answers 501 events 400", func() {
			events := make([]any, hummingbee.MaxBatch+1)
			for i := range events {
				events[i] = orderEvent(fmt.Sprintf("order-status-%d", i))
			}
			resp := post(server, nil, envelopeOf(events...))
			Expect(resp.Status).To(Equal(http.StatusBadRequest))
			Expect(errorCode(resp)).To(Equal("BAD_REQUEST"))
			Expect(server.Events()).To(BeEmpty())
		})

		It("answers a body that is not an envelope 400", func() {
			for _, body := range []string{`not json`, `[]`, `{}`, `{"events": {}}`, `{"events": [], "organization_id": "acme"}`} {
				resp := post(server, nil, []byte(body))
				Expect(resp.Status).To(Equal(http.StatusBadRequest), body)
				Expect(errorCode(resp)).To(Equal("BAD_REQUEST"), body)
			}
		})

		It("answers a body over 2 MiB 413", func() {
			resp := post(server, nil, []byte(`{"events": ["`+strings.Repeat("x", 2<<20)+`"]}`))
			Expect(resp.Status).To(Equal(http.StatusRequestEntityTooLarge))
			Expect(errorCode(resp)).To(Equal("PAYLOAD_TOO_LARGE"))
		})

		It("answers a gzip body that inflates past 2 MiB 413", func() {
			var compressed bytes.Buffer
			zw := gzip.NewWriter(&compressed)
			_, err := zw.Write([]byte(`{"events": ["` + strings.Repeat("x", 3<<20) + `"]}`))
			Expect(err).NotTo(HaveOccurred())
			Expect(zw.Close()).To(Succeed())

			resp := post(server, map[string]string{"Content-Encoding": "gzip"}, compressed.Bytes())
			Expect(resp.Status).To(Equal(http.StatusRequestEntityTooLarge))
		})

		It("accepts an uncompressed body", func() {
			results := resultsOf(post(server, nil, envelopeOf(orderEvent("order-status-1"))))
			Expect(results).To(HaveLen(1))
			Expect(results[0].Status).To(Equal(hummingbee.StatusAccepted))
		})

		It("answers another path 404", func() {
			resp, err := http.Get(server.URL() + "/v1/other")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Body.Close()).To(Succeed())
			Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		})
	})

	Describe("each event", func() {
		It("rejects an event without a subject, naming subject.id as required", func() {
			event := orderEvent("order-status-1")
			event.Subject = hummingbee.Ref{}

			results, err := server.Client().Send(ctx, []hummingbee.Event{event})
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(Equal([]hummingbee.Result{{
				Field: "subject.id", ID: "order-status-1", Reason: "required", Status: hummingbee.StatusRejected,
			}}))
			Expect(server.Events()).To(BeEmpty())
		})

		It("rejects each event that breaks a rule, with its field, and accepts the rest of the batch", func() {
			nonUUID := orderEvent("bad-subject")
			nonUUID.Subject.ID = "order-42"

			future := orderEvent("in-future")
			future.OccurredAt = time.Now().Add(10 * time.Minute)

			tooManyChanges := orderEvent("too-many-changes")
			tooManyChanges.Changes = make([]hummingbee.Change, 101)
			for i := range tooManyChanges.Changes {
				tooManyChanges.Changes[i] = hummingbee.Change{Field: fmt.Sprintf("f%d", i), From: nil, To: i}
			}

			objectValue := orderEvent("object-value")
			objectValue.Changes = []hummingbee.Change{{Field: "address", From: nil, To: map[string]any{"city": "Springfield"}}}

			robot := orderEvent("robot-actor")
			robot.Actor = &hummingbee.Actor{Kind: "robot", Name: "r2"}

			badID := orderEvent("-starts-with-a-dash")

			badType := orderEvent("bad-type")
			badType.Type = "OrderStatusChanged"

			longValue := orderEvent("long-value")
			longValue.Changes = []hummingbee.Change{{Field: "note", From: nil, To: strings.Repeat("x", 1001)}}

			tooManyRelated := orderEvent("too-many-related")
			tooManyRelated.Related = make([]hummingbee.Ref, 51)
			for i := range tooManyRelated.Related {
				tooManyRelated.Related[i] = hummingbee.Ref{ID: customerID, Kind: "customer"}
			}

			badUser := orderEvent("bad-user")
			badUser.Actor = hummingbee.User("person-7")

			badRelated := orderEvent("bad-related")
			badRelated.Related = []hummingbee.Ref{{ID: customerID, Kind: "customer"}, {ID: "nope", Kind: "customer"}}

			events := []hummingbee.Event{
				orderEvent("good-1"), nonUUID, future, tooManyChanges, objectValue, robot,
				badID, badType, longValue, tooManyRelated, badUser, badRelated, orderEvent("good-2"),
			}
			results, err := server.Client().Send(ctx, events)
			Expect(err).NotTo(HaveOccurred())

			type verdict struct{ Field, Reason string }
			verdicts := map[string]verdict{}
			for i, result := range results {
				Expect(result.Index).To(Equal(i))
				Expect(result.ID).To(Equal(events[i].ID))
				verdicts[result.ID] = verdict{result.Field, result.Reason}
				if result.Reason == "" {
					Expect(result.Status).To(Equal(hummingbee.StatusAccepted), result.ID)
				} else {
					Expect(result.Status).To(Equal(hummingbee.StatusRejected), result.ID)
				}
			}
			Expect(verdicts).To(Equal(map[string]verdict{
				"good-1":              {},
				"bad-subject":         {"subject.id", "invalid_uuid"},
				"in-future":           {"occurred_at", "in_future"},
				"too-many-changes":    {"changes", "too_many_changes"},
				"object-value":        {"changes[0].to", "invalid_change_value"},
				"robot-actor":         {"actor.kind", "invalid_actor"},
				"-starts-with-a-dash": {"id", "invalid_id"},
				"bad-type":            {"type", "invalid_type"},
				"long-value":          {"changes[0].to", "invalid_change_value"},
				"too-many-related":    {"related", "too_many_related"},
				"bad-user":            {"actor.id", "invalid_uuid"},
				"bad-related":         {"related[1].id", "invalid_uuid"},
				"good-2":              {},
			}))

			ids := []string{}
			for _, event := range server.Events() {
				ids = append(ids, event.ID)
			}
			Expect(ids).To(Equal([]string{"good-1", "good-2"}))
		})

		It("accepts an event with only the required fields, and the edges of each limit", func() {
			minimal := hummingbee.Event{
				ID:         strings.Repeat("a", 200),
				OccurredAt: time.Now().Add(4 * time.Minute),
				Subject:    hummingbee.Ref{ID: strings.ToUpper(orderID), Kind: "order"},
				Type:       "order.status_changed",
			}
			full := orderEvent("order:1_2.3-4")
			full.Changes = make([]hummingbee.Change, 100)
			for i := range full.Changes {
				full.Changes[i] = hummingbee.Change{Field: fmt.Sprintf("f%d", i), From: true, To: strings.Repeat("é", 1000)}
			}
			full.Related = make([]hummingbee.Ref, 50)
			for i := range full.Related {
				full.Related[i] = hummingbee.Ref{ID: customerID, Kind: "customer"}
			}

			results, err := server.Client().Send(ctx, []hummingbee.Event{minimal, full})
			Expect(err).NotTo(HaveOccurred())
			for _, result := range results {
				Expect(result.Status).To(Equal(hummingbee.StatusAccepted), result.Field+" "+result.Reason)
			}
		})

		It("rejects an id over 200 characters", func() {
			results, err := server.Client().Send(ctx, []hummingbee.Event{orderEvent(strings.Repeat("a", 201))})
			Expect(err).NotTo(HaveOccurred())
			Expect(results[0].Field).To(Equal("id"))
			Expect(results[0].Reason).To(Equal("invalid_id"))
		})

		It("rejects an array entry that is not an object, with an empty id", func() {
			results := resultsOf(post(server, nil, envelopeOf(42, orderEvent("order-status-1"))))
			Expect(results).To(Equal([]hummingbee.Result{
				{Index: 0, Reason: "not_an_object", Status: hummingbee.StatusRejected},
				{ID: "order-status-1", Index: 1, Status: hummingbee.StatusAccepted},
			}))
		})

		It("rejects missing required fields first", func() {
			results := resultsOf(post(server, nil, envelopeOf(
				map[string]any{"subject": map[string]any{"id": orderID, "kind": "order"}},
				map[string]any{"id": "a", "subject": map[string]any{"id": orderID, "kind": "order"}, "type": "order.created"},
				map[string]any{"id": "b", "occurred_at": "2026-11-17T09:30:00Z", "subject": map[string]any{"id": orderID}, "type": "order.created"},
				map[string]any{"id": "c", "occurred_at": "2026-11-17T09:30:00Z", "subject": map[string]any{"id": orderID, "kind": "order"}},
				map[string]any{"id": "d", "occurred_at": "yesterday", "subject": map[string]any{"id": orderID, "kind": "order"}, "type": "order.created"},
			)))
			fields := []string{}
			for _, result := range results {
				Expect(result.Status).To(Equal(hummingbee.StatusRejected))
				fields = append(fields, result.Field+" "+result.Reason)
			}
			Expect(fields).To(Equal([]string{
				"id required", "occurred_at required", "subject.kind required", "type required", "occurred_at invalid_time",
			}))
		})

		It("rejects an id given to RejectID with that field and reason", func() {
			server.RejectID("order-status-1", "changes[0].field", "unknown_field")

			results, err := server.Client().Send(ctx, []hummingbee.Event{orderEvent("order-status-1"), orderEvent("order-status-2")})
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(Equal([]hummingbee.Result{
				{Field: "changes[0].field", ID: "order-status-1", Index: 0, Reason: "unknown_field", Status: hummingbee.StatusRejected},
				{ID: "order-status-2", Index: 1, Status: hummingbee.StatusAccepted},
			}))
		})
	})

	It("records every event from twenty goroutines sending at once", func() {
		client := server.Client()
		var wg sync.WaitGroup
		errs := make([]error, 20)
		for i := range 20 {
			wg.Go(func() {
				_, errs[i] = client.Send(ctx, []hummingbee.Event{orderEvent(fmt.Sprintf("order-status-%d", i))})
			})
		}
		wg.Wait()
		for _, err := range errs {
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(server.Events()).To(HaveLen(20))
		Expect(server.Requests()).To(Equal(20))
	})

	It("returns copies from Events, so a test cannot change what was recorded", func() {
		_, err := server.Client().Send(ctx, []hummingbee.Event{orderEvent("order-status-1")})
		Expect(err).NotTo(HaveOccurred())

		events := server.Events()
		events[0].Actor.Name = "changed"
		events[0].Changes[0].To = "changed"
		events[0].Related[0].Kind = "changed"

		again := server.Events()[0]
		Expect(again.Actor.Name).To(Equal("order-sync"))
		Expect(again.Changes[0].To).To(Equal("paid"))
		Expect(again.Related[0].Kind).To(Equal("customer"))
	})

	It("forgets everything but the token on Reset", func() {
		token := server.Token()
		client := server.Client(hummingbee.WithMaxAttempts(1))
		_, err := client.Send(ctx, []hummingbee.Event{orderEvent("order-status-1")})
		Expect(err).NotTo(HaveOccurred())
		server.RejectID("order-status-2", "id", "spec")
		server.FailNext(3, http.StatusServiceUnavailable)

		server.Reset()
		Expect(server.Events()).To(BeEmpty())
		Expect(server.Requests()).To(BeZero())
		Expect(server.Token()).To(Equal(token))

		results, err := client.Send(ctx, []hummingbee.Event{orderEvent("order-status-1"), orderEvent("order-status-2")})
		Expect(err).NotTo(HaveOccurred())
		Expect(results[0].Status).To(Equal(hummingbee.StatusAccepted))
		Expect(results[1].Status).To(Equal(hummingbee.StatusAccepted))
	})

	It("gives each server its own token and memory", func() {
		other := hummingbeetest.NewServer()
		defer other.Close()
		Expect(other.Token()).NotTo(Equal(server.Token()))
		Expect(other.Token()).To(HavePrefix("hbk_test_"))

		_, err := server.Client().Send(ctx, []hummingbee.Event{orderEvent("order-status-1")})
		Expect(err).NotTo(HaveOccurred())
		Expect(other.Events()).To(BeEmpty())
	})
})
