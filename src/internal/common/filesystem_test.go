package common

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveDirNonexistent(t *testing.T) {
	tmp := filepath.Join(os.TempDir(), "ocf_nonexistent_dir_for_test")
	// ensure it does not exist
	_ = os.RemoveAll(tmp)
	if err := RemoveDir(tmp); err != nil {
		t.Fatalf("RemoveDir on nonexistent dir should not error: %v", err)
	}
}

func TestRemoveDirExisting(t *testing.T) {
	dir, err := os.MkdirTemp("", "ocf_remove_dir_test")
	if err != nil {
		t.Fatal(err)
	}
	// create a nested file
	nested := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(nested, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := RemoveDir(dir); err != nil {
		t.Fatalf("RemoveDir should remove directory: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("expected dir to be removed, stat err: %v", err)
	}
}
