// Package hummingbeetest helps a firm test the code that sends events to
// Hummingbee, without Hummingbee.
//
// [NewServer] starts a fake events endpoint that answers as the real one does
// and records what it accepted:
//
//	server := hummingbeetest.NewServer()
//	defer server.Close()
//
//	results, err := server.Client().Send(ctx, events)
//	// server.Events() holds the accepted events, in arrival order.
//
// It checks the bearer key, the content type and the encoding, keeps the first
// copy of each event id and reports later copies as duplicates, and rejects
// each event that breaks a per-event rule of wire format v1, with the field
// and the reason the real endpoint gives. Code whose events pass against it
// should not see them rejected in production for a rule the format states.
// For shapes the format names no reason for, it answers not_an_object (a value
// that must be an object is not one), not_an_array (changes or related is not
// an array) or invalid_time (occurred_at is not RFC 3339).
//
// [DryRun] returns an HTTP client for [hummingbee.WithHTTPClient] that sends
// nothing: it logs each event and answers it accepted, for a deployment that
// should build its events but not send them.
package hummingbeetest
