
package pow

import (
	"strings"
	"testing"
)

func TestHash(t *testing.T) {
	got := Hash("hello", 0)

	if len(got) != 64 {
		t.Fatalf("expected 64 hex characters, got %d", len(got))
	}
}

func TestSearch(t *testing.T) {
	nonce, hash, found := Search("hello", 3, 0, 100000)

	if !found {
		t.Fatal("expected a valid nonce")
	}

	if !strings.HasPrefix(hash, "000") {
		t.Fatalf("invalid hash: %s", hash)
	}

	if Hash("hello", nonce) != hash {
		t.Fatal("nonce and hash do not match")
	}
}

func TestInvalidDifficulty(t *testing.T) {
	if Valid(strings.Repeat("0", 64), 9) {
		t.Fatal("difficulty 9 must be rejected")
	}
}
