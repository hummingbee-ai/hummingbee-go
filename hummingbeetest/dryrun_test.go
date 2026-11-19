package hummingbeetest_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/hummingbee-ai/hummingbee-go"
	"github.com/hummingbee-ai/hummingbee-go/hummingbeetest"
)

var _ = Describe("DryRun", func() {
	var (
		client *hummingbee.Client
		logs   *bytes.Buffer
	)

	BeforeEach(func() {
		logs = &bytes.Buffer{}
		logger := slog.New(slog.NewJSONHandler(logs, nil))
		// .invalid never resolves, so a request that reached the network would fail.
		client = hummingbee.New("hbk_dry_run",
			hummingbee.WithBaseURL("http://dry-run.invalid"),
			hummingbee.WithHTTPClient(hummingbeetest.DryRun(logger)),
			hummingbee.WithMaxAttempts(1))
	})

	logLines := func() []map[string]any {
		GinkgoHelper()
		var lines []map[string]any
		scanner := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
		for scanner.Scan() {
			var line map[string]any
			Expect(json.Unmarshal(scanner.Bytes(), &line)).To(Succeed())
			lines = append(lines, line)
		}
		return lines
	}

	It("answers every event accepted and logs one line per event, sending nothing", func() {
		events := []hummingbee.Event{orderEvent("order-status-1"), orderEvent("order-status-2")}
		events[1].Subject.Kind = "invoice"
		events[1].Type = "invoice.paid"

		results, err := client.Send(context.Background(), events)
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(Equal([]hummingbee.Result{
			{ID: "order-status-1", Index: 0, Status: hummingbee.StatusAccepted},
			{ID: "order-status-2", Index: 1, Status: hummingbee.StatusAccepted},
		}))

		lines := logLines()
		Expect(lines).To(HaveLen(2))
		for _, line := range lines {
			delete(line, "time")
		}
		Expect(lines).To(Equal([]map[string]any{
			{"id": "order-status-1", "level": "INFO", "msg": "hummingbee dry run", "subject_kind": "order", "type": "order.status_changed"},
			{"id": "order-status-2", "level": "INFO", "msg": "hummingbee dry run", "subject_kind": "invoice", "type": "invoice.paid"},
		}))
	})

	It("never logs a change's values or the ids in refs", func() {
		event := orderEvent("order-status-1")
		event.Changes = []hummingbee.Change{{Field: "email", From: "old-value@example.test", To: "new-value@example.test"}}

		_, err := client.Send(context.Background(), []hummingbee.Event{event})
		Expect(err).NotTo(HaveOccurred())
		Expect(logs.String()).To(ContainSubstring("order-status-1"))
		Expect(logs.String()).NotTo(ContainSubstring("old-value"))
		Expect(logs.String()).NotTo(ContainSubstring("new-value"))
		Expect(logs.String()).NotTo(ContainSubstring(orderID))
		Expect(logs.String()).NotTo(ContainSubstring(customerID))
		Expect(logs.String()).NotTo(ContainSubstring("hbk_dry_run"))
	})
})
