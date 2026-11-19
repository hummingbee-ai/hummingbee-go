package hummingbee_test

import (
	"context"
	"fmt"
	"time"

	"github.com/hummingbee-ai/hummingbee-go"
	"github.com/hummingbee-ai/hummingbee-go/hummingbeetest"
)

// The README's quick start, sent to a fake endpoint. In production the client
// sends to Hummingbee with the sending system's own key.
func Example() {
	server := hummingbeetest.NewServer()
	defer server.Close()
	token := server.Token()
	orgID := "0193a0b2-7c1d-7e3f-9a4b-000000000003"

	client := hummingbee.New(token,
		hummingbee.WithBaseURL(server.URL()),
		hummingbee.WithOrganizationID(orgID))

	// Fields in alphabetical order. Ids in refs are UUIDs.
	results, err := client.Send(context.Background(), []hummingbee.Event{{
		Actor: hummingbee.System("order-sync"),
		Changes: []hummingbee.Change{{
			Field: "status", From: "submitted", To: "paid",
		}},
		ID:         "order-status-0193a0b2-7c1d-7e3f-9a4b-5c6d7e8f9a0b",
		OccurredAt: time.Now(),
		Related: []hummingbee.Ref{
			{ID: "0193a0b2-7c1d-7e3f-9a4b-000000000001", Kind: "customer"},
		},
		Subject: hummingbee.Ref{ID: "0193a0b2-7c1d-7e3f-9a4b-000000000002", Kind: "order"},
		Type:    "order.status_changed",
	}})
	if err != nil {
		fmt.Println("failed to send the order event:", err)
		return
	}
	fmt.Println(results[0].ID, results[0].Status)
	// Output:
	// order-status-0193a0b2-7c1d-7e3f-9a4b-5c6d7e8f9a0b accepted
}

func ExampleNew() {
	client := hummingbee.New("hbk_example_key",
		hummingbee.WithMaxAttempts(3),
		hummingbee.WithOrganizationID("0193a0b2-7c1d-7e3f-9a4b-000000000003"))

	// The client names its endpoint, never its key.
	fmt.Println(client)
	// Output:
	// hummingbee.Client(https://api.hummingbee.ai)
}

func ExampleClient_Send() {
	server := hummingbeetest.NewServer()
	defer server.Close()
	client := server.Client()

	order := func(id, subjectID string) hummingbee.Event {
		return hummingbee.Event{
			Actor:      hummingbee.User("0193a0b2-7c1d-7e3f-9a4b-000000000004"),
			Changes:    []hummingbee.Change{{Field: "status", From: "paid", To: "refunded"}},
			ID:         id,
			OccurredAt: time.Now(),
			Subject:    hummingbee.Ref{ID: subjectID, Kind: "order"},
			Type:       "order.status_changed",
		}
	}
	events := []hummingbee.Event{
		order("order-refund-1", "0193a0b2-7c1d-7e3f-9a4b-000000000002"),
		order("order-refund-1", "0193a0b2-7c1d-7e3f-9a4b-000000000002"), // sent twice
		order("order-refund-2", "order-42"),                             // not a UUID
	}

	results, err := client.Send(context.Background(), events)
	if err != nil {
		fmt.Println("failed to send:", err)
		return
	}
	for _, result := range results {
		switch result.Status {
		case hummingbee.StatusAccepted, hummingbee.StatusDuplicate:
			fmt.Println(result.Index, result.ID, result.Status)
		case hummingbee.StatusRejected:
			fmt.Println(result.Index, result.ID, result.Status, result.Field, result.Reason)
		}
	}
	// Output:
	// 0 order-refund-1 accepted
	// 1 order-refund-1 duplicate
	// 2 order-refund-2 rejected subject.id invalid_uuid
}
