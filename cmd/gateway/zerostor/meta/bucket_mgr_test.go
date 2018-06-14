package meta

import (
	"sort"
	"testing"

	"github.com/minio/minio-go/pkg/policy"
)

type bktMgrConstructor func() (BucketManager, error)

func testBucketMgrRoundTrip(t *testing.T, bktMgr BucketManager,
	constructor bktMgrConstructor) {

	var (
		bucketsToCreate = []string{"bucket_1", "bucket_2", "bucket_3", "bucket_4"}
		err             error
	)
	sort.Strings(bucketsToCreate) // sort it so we can easily compare it

	// create buckets
	for _, bucket := range bucketsToCreate {
		err = bktMgr.Create(bucket)
		if err != nil {
			t.Errorf("failed to create bucket `%v`: %v", bucket, err)
		}
	}

	checkGetBuckets := func(bm BucketManager, expectedBuckets []string, msg string) {
		t.Log(msg)
		for _, bucket := range expectedBuckets {
			// check get bucket
			bkt, err := bm.Get(bucket)
			if err != nil {
				t.Errorf("failed to get bucket `%v`:%v", bucket, err)
			}
			if bkt.Name != bucket {
				t.Errorf("invalid bucket name: %v, expected: %v", bkt.Name, bucket)
			}
		}

		// check get all buckets
		var gotBuckets []string
		allBuckets, err := bm.GetAllBuckets()
		if err != nil {
			t.Fatalf("failed to GetAllBuckets:%v", err)
		}
		for _, bucket := range allBuckets {
			gotBuckets = append(gotBuckets, bucket.Name)
		}

		sort.Strings(gotBuckets) // sort the name, so we can easily compare it

		// compare it with original buckets
		if len(expectedBuckets) != len(gotBuckets) {
			t.Errorf("unexpected getAllBuckets result len: %v, expected: %v", len(gotBuckets),
				len(expectedBuckets))
		}

		for i, bucket := range expectedBuckets {
			if bucket != gotBuckets[i] {
				t.Errorf("invalid bucket name: %v, expected: %v", gotBuckets[i], bucket)
			}
		}
	}

	// check bucket manager, make sure we could get the data
	{
		// check bucket manager which still use in memory data
		checkGetBuckets(bktMgr, bucketsToCreate, "check in memory bucket manager")

		// check bucket manager, with all data being loaded from file
		loadedBktMgr, err := constructor()
		if err != nil {
			t.Fatalf("failed to load bucket manager: %v", err)
		}
		checkGetBuckets(loadedBktMgr, bucketsToCreate, "check on disk bucket manager")
	}

	// delete buckets and then check again
	{
		deleteIdx := 2
		for i := 0; i < deleteIdx; i++ {
			bucket := bucketsToCreate[i]
			err := bktMgr.Del(bucket)
			if err != nil {
				t.Errorf("failed to delete bucket `%v`: %v", bucket, err)
			}
		}
		checkGetBuckets(bktMgr, bucketsToCreate[deleteIdx:],
			"check in memory bucket manager after deletion")

		// check bucket manager, with all data being loaded from file
		loadedBktMgr, err := constructor()
		if err != nil {
			t.Fatalf("failed to load bucket manager: %v", err)
		}
		checkGetBuckets(loadedBktMgr, bucketsToCreate[deleteIdx:],
			"check on disk bucket manager after deletion")
	}
}

func testBucketMgrPolicy(t *testing.T, bktMgr BucketManager, constructor bktMgrConstructor) {
	const (
		bucketName  = "bucket_1"
		policyToSet = policy.BucketPolicyReadOnly
	)

	// create bucket
	err := bktMgr.Create(bucketName)
	if err != nil {
		t.Errorf("failed to create bucket `%v`: %v", bucketName, err)
	}

	// get bucket, make sure it's policy is different than `policyToSet`
	bkt, err := bktMgr.Get(bucketName)
	if err != nil {
		t.Errorf("failed to get bucket `%v`: %v", bucketName, err)
	}

	if bkt.Policy == policyToSet {
		t.Fatalf("bad policy value: %v. expect different policy", bkt.Policy)
	}

	// set policy
	err = bktMgr.SetPolicy(bucketName, policyToSet)
	if err != nil {
		t.Errorf("failed to set bucket policy: %v", err)
	}

	checkBucketPolicy := func(bm BucketManager, msg string) {
		t.Log(msg)
		var bkt *Bucket
		bkt, err = bm.Get(bucketName)
		if err != nil {
			t.Errorf("failed to get bucket `%v`: %v", bucketName, err)
		}
		if bkt.Policy != policyToSet {
			t.Errorf("bad policy value: %v, expected: %v", bkt.Policy, policyToSet)
		}
	}

	checkBucketPolicy(bktMgr, "check in memory bucket manager")

	// check bucket manager, with all data being loaded from file
	loadedBktMgr, err := constructor()
	if err != nil {
		t.Fatalf("failed to load bucket manager: %v", err)
	}

	checkBucketPolicy(loadedBktMgr, "check on disk bucket manager")
}
