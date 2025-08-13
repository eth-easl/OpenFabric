package common

import "testing"

func TestReportErrorNoPanic(t *testing.T) {
	// should not panic on nil
	ReportError(nil, "msg")
}
