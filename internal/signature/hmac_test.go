package signature

import (
	"testing"
)

func TestSignAndVerify(t *testing.T) {
	key := []byte("my-secret-key")
	timestamp := "1704067200"
	body := []byte(`{"event":"order.created"}`)

	sig := Sign(key, timestamp, body)
	if sig == "" {
		t.Fatal("signature should not be empty")
	}

	if !Verify(key, timestamp, body, sig) {
		t.Fatal("Verify should return true for valid signature")
	}
}

func TestVerifyWrongKey(t *testing.T) {
	key := []byte("my-secret-key")
	wrongKey := []byte("wrong-key")
	timestamp := "1704067200"
	body := []byte(`{"event":"order.created"}`)

	sig := Sign(key, timestamp, body)

	if Verify(wrongKey, timestamp, body, sig) {
		t.Fatal("Verify should return false for wrong key")
	}
}

func TestVerifyTamperedBody(t *testing.T) {
	key := []byte("my-secret-key")
	timestamp := "1704067200"
	body := []byte(`{"event":"order.created"}`)
	tampered := []byte(`{"event":"order.deleted"}`)

	sig := Sign(key, timestamp, body)

	if Verify(key, timestamp, tampered, sig) {
		t.Fatal("Verify should return false for tampered body")
	}
}

func TestVerifyTamperedTimestamp(t *testing.T) {
	key := []byte("my-secret-key")
	timestamp := "1704067200"
	body := []byte(`{"event":"order.created"}`)

	sig := Sign(key, timestamp, body)

	if Verify(key, "9999999999", body, sig) {
		t.Fatal("Verify should return false for tampered timestamp")
	}
}

func TestSignDeterministic(t *testing.T) {
	key := []byte("key")
	ts := "12345"
	body := []byte("body")

	sig1 := Sign(key, ts, body)
	sig2 := Sign(key, ts, body)
	if sig1 != sig2 {
		t.Fatal("Sign should be deterministic")
	}
}
