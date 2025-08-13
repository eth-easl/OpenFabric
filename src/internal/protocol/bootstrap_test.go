package protocol

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
)

func TestGetDefaultBootstrapPeersStandalone(t *testing.T) {
	// in standalone mode, should return empty slice even if provided nil
	res := getDefaultBootstrapPeers(nil, "standalone")
	if len(res) != 0 {
		t.Fatalf("expected 0 peers, got %d", len(res))
	}
}

func TestGetDefaultBootstrapPeersLocal(t *testing.T) {
	res := getDefaultBootstrapPeers(nil, "local")
	if len(res) != 1 {
		t.Fatalf("expected 1 local peer, got %d", len(res))
	}
	// ensure it's a valid multiaddr
	if _, err := multiaddr.NewMultiaddr(res[0].String()); err != nil {
		t.Fatalf("invalid multiaddr: %v", err)
	}
}

func TestGetDefaultBootstrapPeersExplicit(t *testing.T) {
	res := getDefaultBootstrapPeers([]string{"/ip4/127.0.0.1/tcp/1234"}, "any")
	if len(res) != 1 {
		t.Fatalf("expected 1 explicit peer, got %d", len(res))
	}
}
