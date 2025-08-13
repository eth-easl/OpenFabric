package process

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

func TestProcessRunWait(t *testing.T) {
	p := NewProcess("/bin/echo", "", false, "hello")
	p.Start()
	if err := p.Wait(); err != nil {
		t.Fatalf("echo failed: %v", err)
	}
}

func TestProcessStreamOutput(t *testing.T) {
	p := NewProcess("/bin/echo", "", false, "stream")
	sc := p.StreamOutput()
	p.Start()
	var out string
	for sc.Scan() {
		out += sc.Text()
	}
	if !strings.Contains(out, "stream") {
		t.Fatalf("expected stream in output, got %q", out)
	}
}

func TestProcessTimeoutKill(t *testing.T) {
	p := NewProcess("/bin/sleep", "", false, "10")
	p.SetTimeout(100 * time.Millisecond)
	p.Start()
	if err := p.Wait(); err == nil {
		t.Fatalf("expected error due to kill/timeout")
	}
}

func TestProcessKillEarly(t *testing.T) {
	p := NewProcess("/bin/sleep", "", false, "2")
	// stream output before starting for coverage of guard path
	_ = p.StreamOutput()
	p.Start()
	// give it a moment to start
	time.Sleep(50 * time.Millisecond)
	p.Kill()
	_ = p.Wait() // may return error; ensure no panic
}

func TestOpenInputStreamGuard(t *testing.T) {
	p := NewProcess("/bin/echo", "", false, "x")
	// open input before start
	w, err := p.OpenInputStream()
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	// write something and close
	bw := bufio.NewWriter(w)
	_, _ = bw.WriteString("hi")
	bw.Flush()
	_ = w.Close()
}
