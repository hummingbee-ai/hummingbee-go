package hummingbee

import "time"

// Event is one change in a firm's software, as wire format v1 carries it.
//
// The fields are in alphabetical order, as the wire format's keys are. Ids in
// refs and user actors are UUIDs, passed as strings so this package needs no
// UUID library.
type Event struct {
	// Actor is who or what made the change. Optional.
	Actor *Actor `json:"actor,omitempty"`
	// Changes lists only the fields that changed, at most 100. Optional.
	Changes []Change `json:"changes,omitempty"`
	// ID is the firm's own id for the event: 1 to 200 of A-Z a-z 0-9 . _ : -,
	// starting with a letter or digit. Hummingbee keeps the first copy of each
	// id, so sending an event again is safe.
	ID string `json:"id"`
	// OccurredAt is when the change happened in the firm's software.
	OccurredAt time.Time `json:"occurred_at"`
	// Related names the other things the event is about, at most 50. Optional.
	Related []Ref `json:"related,omitempty"`
	// Subject is the thing that changed.
	Subject Ref `json:"subject"`
	// Type is dotted lower case, such as "order.status_changed".
	Type string `json:"type"`
}

// Change is one field that changed, with its value before and after.
//
// From and To are encoded as given: a string of at most 1,000 characters, a
// number, a boolean or nil, which encodes as null.
type Change struct {
	Field string `json:"field"`
	From  any    `json:"from"`
	To    any    `json:"to"`
}

// Ref names a thing in the firm's software by its own UUID and its kind, such
// as "order" or "customer".
type Ref struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// The actor kinds wire format v1 knows.
const (
	ActorKindSystem = "system"
	ActorKindUser   = "user"
)

// Actor is who or what made a change: a system by name, or a person by UUID.
// Build one with [System] or [User].
type Actor struct {
	ID   string `json:"id,omitempty"`
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
}

// System returns an actor for the part of the firm's software that made the
// change, such as "order-sync".
func System(name string) *Actor {
	return &Actor{Kind: ActorKindSystem, Name: name}
}

// User returns an actor for the person who made the change, by their UUID in
// the firm's software.
func User(id string) *Actor {
	return &Actor{ID: id, Kind: ActorKindUser}
}
