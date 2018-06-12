package meta

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
)

const (
	bucketParent = "."
)

type MongoBucketManager struct {
	mms *MongoMetaStor // mongo meta storage
}

func NewMongoBucketManager(mms *MongoMetaStor) *MongoBucketManager {
	return &MongoBucketManager{
		mms: mms,
	}
}

func (mbm *MongoBucketManager) Create(bucket string) error {
	return mbm.mms.createBucket(bucket)
}

func (mbm *MongoBucketManager) Get(bucket string) (*Bucket, error) {
	return mbm.mms.getBucket(bucket)
}

func (mbm *MongoBucketManager) GetAllBuckets() ([]Bucket, error) {
	return mbm.mms.getAllBuckets()
}

func (mbm *MongoBucketManager) Del(bucket string) error {
	return mbm.mms.delBucket(bucket)
}

func (mbm *MongoBucketManager) SetPolicy(bucket string, pol policy.BucketPolicy) error {
	return mbm.mms.setBucketPolicy(bucket, pol)
}

// createBucket implements BucketManager.Create
func (mm *MongoMetaStor) createBucket(bucket string) error {
	ses := mm.getSession()
	defer ses.Close()

	return mm.getCol(ses).Insert(&mongoEntry{
		ID:           bucket,
		Parent:       bucketParent,
		CreatedTime:  time.Now(),
		BucketPolicy: defaultBucketPolicy,
	})
}

// getBucket implements BucketManager.Get interface
func (mm *MongoMetaStor) getBucket(bucket string) (*Bucket, error) {
	ses := mm.getSession()
	defer ses.Close()

	var entry mongoEntry

	err := mm.getCol(ses).FindId(bucket).One(&entry)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, minio.BucketNotFound{}
		}
		return nil, err
	}

	bkt := entry.toBucketObj()
	return &bkt, nil
}

func (mm *MongoMetaStor) getAllBuckets() ([]Bucket, error) {
	ses := mm.getSession()
	defer ses.Close()

	// get from db
	var entries []mongoEntry
	err := mm.getCol(ses).Find(bson.M{
		"parent": bucketParent,
	}).All(&entries)
	if err != nil {
		return nil, err
	}

	// construct the data
	var bkts []Bucket
	for _, e := range entries {
		bkts = append(bkts, e.toBucketObj())
	}

	return bkts, nil
}

func (mm *MongoMetaStor) delBucket(bucket string) error {
	ses := mm.getSession()
	defer ses.Close()

	return mm.getCol(ses).RemoveId(bucket)
}

func (mm *MongoMetaStor) setBucketPolicy(bucket string, pol policy.BucketPolicy) error {
	ses := mm.getSession()
	defer ses.Close()

	return mm.getCol(ses).UpdateId(bucket, bson.M{
		"$set": bson.M{"policy": pol},
	})
}

var (
	_ BucketManager = (*MongoBucketManager)(nil)
)
