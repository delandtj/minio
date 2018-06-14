package meta

import (
	"bytes"
	"fmt"
	//"io/ioutil"
	//"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

func testRoundTrip(t *testing.T, stor Storage, bucket string) {
	var (
		namespace = []byte("ns")
		key       = []byte(path.Join(bucket, "foo"))
		data      = []byte("bar")
	)

	// ensure metadata is not there yet
	_, err := stor.Get(namespace, key)
	if err != db.ErrNotFound {
		t.Errorf("unexpected error: %v, expected: %v", err, db.ErrNotFound)
	}

	// set the metadata
	err = stor.Set(namespace, key, data)
	if err != nil {
		t.Errorf("unexpected error when set meta: %v", err)
	}

	// get it back
	storedData, err := stor.Get(namespace, key)
	if err != nil {
		t.Errorf("unexpected error when set meta: %v", err)
	}
	// check stored value
	if storedData == nil {
		t.Errorf("storedData shouldn't be nil")
	}
	if !bytes.Equal(data, storedData) {
		t.Errorf("stored data is different")
	}

	// delete the metadata
	err = stor.Delete(namespace, key)
	if err != nil {
		t.Errorf("failed to delete meta: %v", err)
	}

	// make sure we can't get it back
	_, err = stor.Get(namespace, key)
	if err != db.ErrNotFound {
		t.Errorf("unexpected error when getting deleted meta: %v, expected: %v", err, db.ErrNotFound)
	}
}

/*
// Test that filemeta could handle dir properly
func TestFilemetaHandleDir(t *testing.T) {
	const (
		bucket = "bucket"
		dir    = "dir"
	)
	var (
		namespace = []byte("ns")
		metadata  = []byte("*****")
	)

	fm, _, cleanup, err := newTestFilemeta(bucket)
	if err != nil {
		t.Fatalf("failed to create test filemeta: %v", err)
	}
	defer cleanup()

	// set meta with key that contains dir
	err = fm.Set(namespace, []byte(filepath.Join(bucket, dir, "filename")), metadata)
	if err != nil {
		t.Errorf("failed to set meta: %v", err)
	}

	_, err = fm.Get(namespace, []byte(filepath.Join(bucket, dir)))
	if err != nil {
		t.Errorf("failed to get meta: %v", err)
	}
}
*/
// Test ListObjects under bucket
// Test ListObjects of a subsubdir
func testMetaStorListObjects(t *testing.T, stor Storage, marshalFuncPair *encoding.MarshalFuncPair, bucket string) {
	var (
		namespace = []byte("ns")
	)

	// populate the data
	keys := []string{
		"file1",
		"file2",
		"firstdir/file1",
		"firstdir/file2",
		"firstdir/seconddir/file1",
		"firstdir/seconddir/file2",
		"anotherdir/file1",
	}
	for _, key := range keys {
		key = bucket + "/" + key
		metadata, err := marshalFuncPair.Marshal(metatypes.Metadata{
			Namespace:     namespace,
			Key:           []byte(key),
			Size:          4096,
			CreationEpoch: time.Now().UnixNano(),
		})
		if err != nil {
			t.Fatalf("failed to generate metadata for key `%v`: %v", key, err)
		}

		err = stor.Set(namespace, []byte(key), metadata)
		if err != nil {
			t.Fatalf("failed to set metadata for key `%v`: %v", key, err)
		}
	}

	testCases := []struct {
		name      string
		prefix    string
		dirs      []string
		files     []string
		delimiter string
	}{
		{
			name:      "list objects of bucket",
			prefix:    "",
			dirs:      []string{"firstdir/", "anotherdir/"},
			files:     []string{"file1", "file2"},
			delimiter: "/",
		},
		{
			name:      "list of objects in dir",
			prefix:    "firstdir/",
			dirs:      []string{"firstdir/seconddir/"},
			files:     []string{"firstdir/file1", "firstdir/file2"},
			delimiter: "/",
		},
		{
			name:      "list of objects in subdir",
			prefix:    "firstdir/seconddir/",
			files:     []string{"firstdir/seconddir/file1", "firstdir/seconddir/file2"},
			delimiter: "/",
		},

		{
			name:      "prefix = dir without trailing slash, return that dir",
			prefix:    "firstdir",
			dirs:      []string{"firstdir/"},
			delimiter: "/",
		},
		{
			name:      "for file, return only that file",
			prefix:    "file1",
			files:     []string{"file1"},
			delimiter: "/",
		},
		{
			name:      "Recursive",
			prefix:    "",
			files:     keys,
			delimiter: "",
		},
	}

	const (
		marker  = ""
		maxKeys = 1000
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := stor.ListObjects(bucket, tc.prefix, marker, tc.delimiter, maxKeys)
			if err != nil {
				t.Errorf("failed to ListObject for prefix=%v: %v", tc.prefix, err)
			}

			if err := compareStringArrs(tc.dirs, res.Prefixes); err != nil {
				t.Errorf("invalid dirs result: %v", err)
			}
			var files []string
			for _, obj := range res.Objects {
				files = append(files, obj.Name)
			}
			if err := compareStringArrs(tc.files, files); err != nil {
				t.Errorf("invalid files result: %v", err)
			}
		})
	}
}

func compareStringArrs(arr1, arr2 []string) error {
	if len(arr1) != len(arr2) {
		return fmt.Errorf("different length : %v and %v", len(arr1), len(arr2))
	}
	sort.Strings(arr1)
	sort.Strings(arr2)

	for i, elem := range arr1 {
		if elem != arr2[i] {
			return fmt.Errorf("elem %v different : `%v` and `%v`", i, elem, arr2[i])
		}
	}
	return nil
}
