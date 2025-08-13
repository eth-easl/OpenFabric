package common

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetHomePathCreatesDir(t *testing.T) {
	// override HOME to a temp dir
	tmp, err := os.MkdirTemp("", "ocf_home_test")
	if err != nil {
		t.Fatal(err)
	}
	old := os.Getenv("HOME")
	t.Cleanup(func() { _ = os.Setenv("HOME", old) })
	_ = os.Setenv("HOME", tmp)

	p := GetHomePath()
	if !strings.HasSuffix(p, ".ocfcore") {
		t.Fatalf("expected .ocfcore path, got %s", p)
	}
	if st, err := os.Stat(p); err != nil || !st.IsDir() {
		t.Fatalf("expected directory to be created: %v, %v", st, err)
	}
}

func TestGetDBPath(t *testing.T) {
	// fake HOME
	tmp, _ := os.MkdirTemp("", "ocf_home_test2")
	old := os.Getenv("HOME")
	t.Cleanup(func() { _ = os.Setenv("HOME", old) })
	_ = os.Setenv("HOME", tmp)

	db := GetDBPath("node123")
	if !strings.Contains(db, filepath.Join(".ocfcore", "ocfcore.node123.db")) {
		t.Fatalf("unexpected db path: %s", db)
	}
}
