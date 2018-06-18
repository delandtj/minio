package multipart

import (
	"testing"
)

func TestMongoMetaRoundTrip(t *testing.T) {
	mm, cleanup := newTestMongoMetaMgr(t)
	defer cleanup()

	testMetaRoundTrip(t, mm)
}

func TestMongoMetaListUploads(t *testing.T) {
	mm, cleanup := newTestMongoMetaMgr(t)
	defer cleanup()

	testMetaListUploads(t, mm)
}

func newTestMongoMetaMgr(t *testing.T) (*mongoMetaMgr, func()) {
	mm, err := NewMongoMetaMgr("localhost:27017", "minio_multipart")
	if err != nil {
		t.Fatalf("failed to create mongo meta manager: %v", err)
	}

	mm.dropDB()

	return mm, func() {

	}
}
