package zerostor

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"time"
)

type bucket struct {
	Name    string
	Created time.Time
}

func newBucket(name, dir string) (*bucket, error) {
	b := &bucket{
		Name:    name,
		Created: time.Now(),
	}

	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		fileMetaPerm)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return b, gob.NewEncoder(f).Encode(b)
}

func newBucketFromFile(dir, filename string) (*bucket, error) {
	f, err := os.Open(filepath.Join(dir, filename))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var bkt bucket
	err = gob.NewDecoder(f).Decode(&bkt)
	return &bkt, err
}
