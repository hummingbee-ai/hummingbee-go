// Package hummingbee sends events from a firm's own software to Hummingbee.
//
// An event says that something changed, the moment it changed: an order's
// status, a refund, a finished step. It names the things it is about by the
// firm's own ids and lists only the fields that changed. Hummingbee looks up
// anything else when a task needs it.
//
// # Quick start
//
//	client := hummingbee.New(token, hummingbee.WithOrganizationID(orgID))
//
//	results, err := client.Send(ctx, []hummingbee.Event{{
//		Actor: hummingbee.System("order-sync"),
//		Changes: []hummingbee.Change{{
//			Field: "status", From: "submitted", To: "paid",
//		}},
//		ID:         "order-status-" + change.ID,
//		OccurredAt: change.CreatedAt,
//		Related: []hummingbee.Ref{
//			{ID: order.CustomerID, Kind: "customer"},
//		},
//		Subject: hummingbee.Ref{ID: order.ID, Kind: "order"},
//		Type:    "order.status_changed",
//	}})
//	if err != nil {
//		return fmt.Errorf("failed to send order %s event: %w", order.ID, err)
//	}
//
// [Client.Send] posts up to [MaxBatch] events in one gzip-compressed request
// and returns one [Result] per event, in the order given: accepted, a
// duplicate, or rejected with the field and a reason.
//
// # Sending twice is safe
//
// The event id is the firm's own, and Hummingbee keeps the first copy of each
// one. Send retries transport errors and the statuses that are safe to retry,
// with backoff, and a retry or a restart can never make a duplicate: a copy
// already kept comes back as [StatusDuplicate].
//
// # Testing
//
// Package hummingbeetest holds a fake events server that records what was sent,
// for a firm's own tests, and a dry-run HTTP client that logs events without
// sending them.
package hummingbee
