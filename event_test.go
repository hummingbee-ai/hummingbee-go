package hummingbee_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/hummingbee-ai/hummingbee-go"
)

const (
	orderID    = "0193a0b2-7c1d-7e3f-9a4b-000000000002"
	customerID = "0193a0b2-7c1d-7e3f-9a4b-000000000001"
	userID     = "0193a0b2-7c1d-7e3f-9a4b-000000000004"
)

// orderEvent is the design's example event, rewritten with an order.
func orderEvent() hummingbee.Event {
	return hummingbee.Event{
		Actor: hummingbee.System("order-sync"),
		Changes: []hummingbee.Change{{
			Field: "status", From: "submitted", To: "paid",
		}},
		ID:         "order-status-0193a0b2-7c1d-7e3f-9a4b-5c6d7e8f9a0b",
		OccurredAt: time.Date(2026, 11, 17, 9, 30, 0, 0, time.UTC),
		Related: []hummingbee.Ref{
			{ID: customerID, Kind: "customer"},
		},
		Subject: hummingbee.Ref{ID: orderID, Kind: "order"},
		Type:    "order.status_changed",
	}
}

const orderEventJSON = `{` +
	`"actor":{"kind":"system","name":"order-sync"},` +
	`"changes":[{"field":"status","from":"submitted","to":"paid"}],` +
	`"id":"order-status-0193a0b2-7c1d-7e3f-9a4b-5c6d7e8f9a0b",` +
	`"occurred_at":"2026-11-17T09:30:00Z",` +
	`"related":[{"id":"0193a0b2-7c1d-7e3f-9a4b-000000000001","kind":"customer"}],` +
	`"subject":{"id":"0193a0b2-7c1d-7e3f-9a4b-000000000002","kind":"order"},` +
	`"type":"order.status_changed"` +
	`}`

func encode(value any) string {
	GinkgoHelper()
	b, err := json.Marshal(value)
	Expect(err).NotTo(HaveOccurred())
	return string(b)
}

var _ = Describe("Event", func() {
	It("encodes the design's example event to the wire format byte for byte", func() {
		Expect(encode(orderEvent())).To(Equal(orderEventJSON))
	})

	It("encodes occurred_at in RFC 3339, keeping its offset", func() {
		event := orderEvent()
		event.OccurredAt = time.Date(2026, 11, 17, 20, 30, 0, 0, time.FixedZone("", 11*60*60))
		Expect(encode(event)).To(ContainSubstring(`"occurred_at":"2026-11-17T20:30:00+11:00"`))
	})

	It("leaves out actor, changes and related when they are empty", func() {
		event := orderEvent()
		event.Actor = nil
		event.Changes = nil
		event.Related = nil
		Expect(encode(event)).To(Equal(`{` +
			`"id":"order-status-0193a0b2-7c1d-7e3f-9a4b-5c6d7e8f9a0b",` +
			`"occurred_at":"2026-11-17T09:30:00Z",` +
			`"subject":{"id":"0193a0b2-7c1d-7e3f-9a4b-000000000002","kind":"order"},` +
			`"type":"order.status_changed"` +
			`}`))
	})

	It("encodes a nil change value as null", func() {
		change := hummingbee.Change{Field: "status", From: nil, To: "paid"}
		Expect(encode(change)).To(Equal(`{"field":"status","from":null,"to":"paid"}`))
	})

	It("encodes numbers and booleans in changes as given", func() {
		change := hummingbee.Change{Field: "total_cents", From: 1250, To: 0}
		Expect(encode(change)).To(Equal(`{"field":"total_cents","from":1250,"to":0}`))
		change = hummingbee.Change{Field: "is_paid", From: false, To: true}
		Expect(encode(change)).To(Equal(`{"field":"is_paid","from":false,"to":true}`))
	})

	It("decodes a wire-format event back into an equal Event", func() {
		var event hummingbee.Event
		Expect(json.Unmarshal([]byte(orderEventJSON), &event)).To(Succeed())
		Expect(event).To(Equal(orderEvent()))
	})
})

var _ = Describe("Actor", func() {
	It("builds a system actor with its name", func() {
		Expect(encode(hummingbee.System("order-sync"))).To(Equal(`{"kind":"system","name":"order-sync"}`))
	})

	It("builds a user actor with its id", func() {
		Expect(encode(hummingbee.User(userID))).To(Equal(`{"id":"` + userID + `","kind":"user"}`))
	})
})
