package hummingbeetest_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHummingbeetest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "hummingbeetest")
}
