package common

import "testing"

func TestDictionaryToBytes(t *testing.T) {
	data := map[string]interface{}{"a": 1, "b": "x"}
	b, err := DictionaryToBytes(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) == 0 || string(b)[0] != '{' {
		t.Fatalf("expected json object, got: %s", string(b))
	}
}
