package signature

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Sign computes an HMAC-SHA256 signature of "<timestamp>.<body>" using the given key.
// The result is hex-encoded.
func Sign(key []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks that the given signature matches the HMAC-SHA256 of "<timestamp>.<body>".
// Uses constant-time comparison to prevent timing attacks.
func Verify(key []byte, timestamp string, body []byte, signature string) bool {
	expected := Sign(key, timestamp, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
