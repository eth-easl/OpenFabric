package protocol

import (
	"testing"
	"time"
)

func TestBackoffBaseDelay(t *testing.T) {
	min := 5 * time.Second
	max := 2 * time.Minute

	testCases := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{name: "zero attempt defaults to min", attempt: 0, want: min},
		{name: "first attempt returns min", attempt: 1, want: min},
		{name: "second attempt doubles", attempt: 2, want: 2 * min},
		{name: "third attempt doubles again", attempt: 3, want: 4 * min},
		{name: "doubling capped at max", attempt: 6, want: max},
	}

	for _, tc := range testCases {
		got := backoffBaseDelay(tc.attempt, min, max)
		if got != tc.want {
			t.Fatalf("%s: backoffBaseDelay(%d) = %s, want %s", tc.name, tc.attempt, got, tc.want)
		}
	}
}
