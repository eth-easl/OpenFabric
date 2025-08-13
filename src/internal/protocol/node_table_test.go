package protocol

import (
	"encoding/json"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func TestUpdateNodeTableHookAndGetPeer(t *testing.T) {
	_ = GetNodeTable()
	p := Peer{ID: "peer1", PublicAddress: "1.2.3.4"}
	b, _ := json.Marshal(p)
	UpdateNodeTableHook(ds.NewKey("peer1"), b)

	got, err := GetPeerFromTable("peer1")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got.PublicAddress != "1.2.3.4" {
		t.Fatalf("unexpected peer: %+v", got)
	}
}

func TestDeleteNodeTableHook(t *testing.T) {
	table := GetNodeTable()
	p := Peer{ID: "peer2", PublicAddress: "5.6.7.8"}
	b, _ := json.Marshal(p)
	UpdateNodeTableHook(ds.NewKey("peer2"), b)
	DeleteNodeTableHook(ds.NewKey("peer2"))
	if _, ok := (*table)["/peer2"]; ok {
		t.Fatalf("expected peer2 deleted")
	}
}
