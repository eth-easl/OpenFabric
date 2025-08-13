package common

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRemoteGET(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer s.Close()

	b, err := RemoteGET(s.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != "ok" {
		t.Fatalf("unexpected body: %s", string(b))
	}
}
