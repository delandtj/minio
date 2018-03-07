package zerostor

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/pkg/policy"
)

const (
	// default bucket policy.
	// disallow public access by default
	defaultBucketPolicy = policy.BucketPolicyNone
)

type bucket struct {
	Name     string
	Created  time.Time
	Policy   policy.BucketPolicy
	filename string
}

func newBucket(name, dir string) (*bucket, error) {
	b := &bucket{
		Name:     name,
		filename: filepath.Join(dir, name),
		Created:  time.Now(),
		Policy:   defaultBucketPolicy,
	}
	return b, b.save()
}

func newBucketFromFile(dir, filename string) (*bucket, error) {
	filepath := filepath.Join(dir, filename)
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var bkt bucket
	if err = gob.NewDecoder(f).Decode(&bkt); err != nil {
		return nil, err
	}
	bkt.filename = filepath
	return &bkt, nil
}

func (b *bucket) save() error {
	f, err := os.OpenFile(b.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMetaPerm)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(b)
}
