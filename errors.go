package hummingbee

import "fmt"

// ErrTooManyEvents is returned, before any request, when one [Client.Send] is
// given more than [MaxBatch] events. Send the events in several calls.
var ErrTooManyEvents = fmt.Errorf("hummingbee: more than %d events in one Send", MaxBatch)

// APIError is a response from the events endpoint that is not a success: an
// error envelope, or the status alone when the body is not one. Send also
// returns it after the last attempt at a status it retries.
type APIError struct {
	// Code is the server's error code, such as "UNAUTHENTICATED". It is empty
	// when the body was not an error envelope, such as a proxy's HTML page.
	Code string
	// Message is the server's message, or the status text when the body was not
	// an error envelope.
	Message string
	// StatusCode is the HTTP status, such as 401.
	StatusCode int

	retryAfter string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("events endpoint answered %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("events endpoint answered %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// permanentError is a failure that sending the same bytes again would meet
// again, so it is never retried: a request that cannot be built, or a success
// status whose body, read in full, does not decode or whose results do not
// number the events sent. A body that breaks off while it is read is a
// transport error, not a permanentError.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }
