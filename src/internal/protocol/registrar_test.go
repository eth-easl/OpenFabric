package protocol

import "testing"

func TestLocalServiceSnapshot(t *testing.T) {
	// start with empty registry
	localServices = nil
	addLocalService(Service{Name: "llm", Host: "localhost", Port: "8000", IdentityGroup: []string{"model=a"}})
	addLocalService(Service{Name: "llm", Host: "localhost", Port: "8000", IdentityGroup: []string{"model=b"}})

	snap := snapshotLocalServices()
	if len(snap) != 1 {
		t.Fatalf("expected 1 service after dedupe, got %d", len(snap))
	}
	if len(snap[0].IdentityGroup) != 2 {
		t.Fatalf("expected merged identity groups, got %v", snap[0].IdentityGroup)
	}
}
