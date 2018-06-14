package meta

import (
	"testing"
)

func TestMongoBucketMgrRoundtrip(t *testing.T) {
	bktMgr, constructor, cleanup, err := newMongoBktMgr()
	if err != nil {
		t.Fatalf("failed to create test mongo bucket manager: %v", err)
	}
	defer cleanup()

	testBucketMgrRoundTrip(t, bktMgr, constructor)
}

func TestMongoBucketMgrPolicy(t *testing.T) {
	bktMgr, constructor, cleanup, err := newMongoBktMgr()
	if err != nil {
		t.Fatalf("failed to create test mongo bucket manager: %v", err)
	}
	defer cleanup()

	testBucketMgrPolicy(t, bktMgr, constructor)
}

func newMongoBktMgr() (bm BucketManager, constr bktMgrConstructor, cleanup func(), err error) {
	var mStor *MongoMetaStor

	mStor, _, cleanup, err = newMongoMetaStor()
	if err != nil {
		return
	}

	bm = mStor.BucketManager()

	constr = func() (BucketManager, error) {
		return mStor.BucketManager(), nil
	}
	return
}
