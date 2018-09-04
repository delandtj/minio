package meta

import (
	"testing"

	"github.com/threefoldtech/0-stor/client/metastor/encoding"
)

func TestMongoStorRoundTrip(t *testing.T) {
	bucket := "bucket"

	mgoStor, _, cleanup, err := newMongoMetaStor()
	if err != nil {
		t.Fatalf("failed to create mongo meta stor: %v", err)
	}
	defer cleanup()

	testRoundTrip(t, mgoStor, bucket)
}

func TestMongoStorListObjects(t *testing.T) {
	bucket := "bucket"

	mgoStor, marshalFnPair, cleanup, err := newMongoMetaStor()
	if err != nil {
		t.Fatalf("failed to create mongo meta stor: %v", err)
	}
	defer cleanup()

	testMetaStorListObjects(t, mgoStor, marshalFnPair, bucket)
}

func newMongoMetaStor() (mStor *MongoMetaStor, marshalFnPair *encoding.MarshalFuncPair, cleanup func(), err error) {
	marshalFnPair, err = encoding.NewMarshalFuncPair(encoding.MarshalTypeProtobuf)
	if err != nil {
		return
	}

	mStor, err = NewMongoMetaStor("localhost:27017", "minio_zerostor", marshalFnPair)
	if err != nil {
		return
	}
	mStor.dropDB()

	cleanup = func() {
		mStor.Close()
	}

	return
}
