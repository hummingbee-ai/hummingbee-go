package hummingbee

import (
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL is Hummingbee's production API.
const DefaultBaseURL = "https://api.hummingbee.ai"

const (
	defaultMaxAttempts = 5
	defaultTimeout     = 30 * time.Second
)

// Client sends events to Hummingbee's events endpoint. Build one with [New],
// once, and share it: it is safe to use from several goroutines.
type Client struct {
	baseURL        string
	httpClient     *http.Client
	maxAttempts    int
	organizationID string
	token          string
}

// Option configures a [Client] in [New].
type Option func(*Client)

// New returns a client that sends with token, the sending system's own key.
func New(token string, options ...Option) *Client {
	c := &Client{
		baseURL:     DefaultBaseURL,
		httpClient:  &http.Client{Timeout: defaultTimeout},
		maxAttempts: defaultMaxAttempts,
		token:       token,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// WithBaseURL sends to another deployment of the API, such as staging, instead
// of [DefaultBaseURL]. The client posts to url + "/v1/events".
func WithBaseURL(url string) Option {
	return func(c *Client) {
		c.baseURL = strings.TrimRight(url, "/")
	}
}

// WithHTTPClient replaces the default HTTP client, which has a 30 second
// timeout per attempt, for a proxy, tracing or the dry run in
// hummingbeetest. A nil client keeps the default.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithMaxAttempts sets how many times [Client.Send] tries one batch,
// including the first. The default is 5. Less than 1 means 1.
func WithMaxAttempts(n int) Option {
	return func(c *Client) {
		c.maxAttempts = max(n, 1)
	}
}

// WithOrganizationID sends events into one organisation, by its UUID. Without
// it, events go to the key's own organisation.
func WithOrganizationID(id string) Option {
	return func(c *Client) {
		c.organizationID = id
	}
}

// String names the client by its base URL. It never shows the token.
func (c *Client) String() string {
	return "hummingbee.Client(" + c.baseURL + ")"
}

// GoString keeps %#v from printing the token.
func (c *Client) GoString() string {
	return c.String()
}
