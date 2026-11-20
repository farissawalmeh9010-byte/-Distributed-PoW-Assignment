
package pow

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// Hash calculates SHA-256(data + decimal nonce).
func Hash(data string, nonce uint64) string {
	input := data + strconv.FormatUint(nonce, 10)
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// Valid checks whether the hash starts with
// the required number of hexadecimal zeros.
func Valid(hash string, difficulty int) bool {
	if difficulty < 1 || difficulty > 8 {
		return false
	}
	return strings.HasPrefix(hash, strings.Repeat("0", difficulty))
}

// Search checks the half-open range [start, end).
// The caller must manage cancellation when using this
// in a long-running distributed search.
func Search(data string, difficulty int, start, end uint64) (uint64, string, bool) {
	for nonce := start; nonce < end; nonce++ {
		hash := Hash(data, nonce)
		if Valid(hash, difficulty) {
			return nonce, hash, true
		}
	}
	return 0, "", false
}
