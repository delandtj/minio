package meta

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// MongoMetaStor defines metadata storage with mongodb as backend
type MongoMetaStor struct {
	ses      *mgo.Session               // mongodb main session
	dbName   string                     // mongodb DB name
	encodeFn encoding.MarshalMetadata   // encode function
	decodeFn encoding.UnmarshalMetadata // decode function
}

// mongoEntry defines metadata entry in the mongodb
type mongoEntry struct {
	ID       string `bson:"_id,omitempty"`
	Metadata []byte `bson:"data,omitempty"`
	Parent   string `bson:"parent"`

	// bucket-only field
	CreatedTime  time.Time           `bson:"created_time,omitempty"`
	BucketPolicy policy.BucketPolicy `bson:"bucket_policy,omitempty"`
}

func (me *mongoEntry) Key() string {
	return string(me.ID)
}

func (me *mongoEntry) isDir() bool {
	return len(me.Metadata) == 0
}

func (me *mongoEntry) toBucketObj() Bucket {
	return Bucket{
		Name:    me.Key(),
		Created: me.CreatedTime,
		Policy:  me.BucketPolicy,
	}

}

// NewMongoMetaStor creates new mongodb metadata storage
func NewMongoMetaStor(url, dbName string, marshalFnPair *encoding.MarshalFuncPair) (*MongoMetaStor, error) {
	ses, err := mgo.Dial(url)
	if err != nil {
		return nil, err
	}

	return &MongoMetaStor{
		ses:      ses,
		dbName:   dbName,
		encodeFn: marshalFnPair.Marshal,
		decodeFn: marshalFnPair.Unmarshal,
	}, nil
}

// Set implements metastor.DB.Set interface
func (mm *MongoMetaStor) Set(namespace, key, metadata []byte) error {
	ses := mm.getSession()
	defer ses.Close()

	col := mm.getCol(ses)
	if err := mm.set(col, key, metadata); err != nil {
		return err
	}

	return mm.checkCreateParent(string(key), col)
}

func (mm *MongoMetaStor) set(col *mgo.Collection, key, metadata []byte) error {
	parent := mm.parent(string(key))
	if parent != "." && parent != "" { // TODO handle it somewhere else
		parent = parent + "/"
	}
	id := string(key)
	_, err := col.UpsertId(id, &mongoEntry{
		ID:       id,
		Metadata: metadata,
		Parent:   parent,
	})
	return err
}

// Get implements mestator.DB.Get interface
func (mm *MongoMetaStor) Get(namespace, key []byte) ([]byte, error) {
	ses := mm.getSession()

	entry, err := mm.get(ses, key)

	ses.Close()

	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, db.ErrNotFound
		}
		return nil, err
	}

	return entry.Metadata, err
}

func (mm *MongoMetaStor) get(ses *mgo.Session, key []byte) (*mongoEntry, error) {
	var entry mongoEntry

	err := mm.getCol(ses).FindId(string(key)).One(&entry)
	if err != nil {
		return nil, err
	}

	return &entry, nil
}

// Delete implements metastor.DB.Delete interface
func (mm *MongoMetaStor) Delete(namespace, key []byte) error {
	ses := mm.getSession()

	err := mm.getCol(ses).RemoveId(string(key))
	ses.Close()

	return err
}

// Update implements metastor.DB.Update interface
func (mm *MongoMetaStor) Update(namespace, key []byte, cb db.UpdateCallback) error {
	return fmt.Errorf("Update is not implemented yet")
}

// ListKeys implements metastor.DB.ListKeys
func (mm *MongoMetaStor) ListKeys(namespace []byte, cb db.ListCallback) error {
	return fmt.Errorf("ListKeys is not implemented yet")

}

// GetDecodeMeta implements Storage.GetDecodeMeta interface
func (mm *MongoMetaStor) GetDecodeMeta(key []byte) (*metatypes.Metadata, error) {
	ses := mm.getSession()
	defer ses.Close()

	entry, err := mm.get(ses, key)
	if err != nil {
		return nil, err
	}

	var md metatypes.Metadata

	err = mm.decodeFn(entry.Metadata, &md)
	return &md, err
}

// Encode implements Storage.Encode
func (mm *MongoMetaStor) Encode(md metatypes.Metadata) ([]byte, error) {
	return mm.encodeFn(md)
}

// ListObjects implements Storage.ListObjects
func (mm *MongoMetaStor) ListObjects(bucket, prefix, marker, delimiter string,
	maxKeys int) (minio.ListObjectsInfo, error) {
	ses := mm.getSession()
	defer ses.Close()

	var (
		result minio.ListObjectsInfo
		root   = path.Join(bucket, prefix)
		md     metatypes.Metadata
	)

	// get the metadata
	entry, err := mm.get(ses, []byte(root))
	if err != nil {
		if err == mgo.ErrNotFound {
			return result, db.ErrNotFound
		}
		return result, err
	}

	// if only a file, return directly
	if !entry.isDir() {
		err = mm.decodeFn(entry.Metadata, &md)
		if err != nil {
			return result, err
		}
		result.Objects = append(result.Objects, CreateObjectInfo(bucket, prefix, &md))
		return result, nil
	}

	// when requesting for a dir, the prefix must
	// ended with trailing slash
	if prefix != "" && !strings.HasSuffix(prefix, "/") && delimiter != "" {
		result.Prefixes = []string{prefix + "/"}
		return result, nil
	}
	return mm.listDir(ses, bucket, prefix, marker, delimiter, maxKeys)
}

func (mm *MongoMetaStor) listDir(ses *mgo.Session, bucket, prefix, marker, delimiter string,
	maxKeys int) (result minio.ListObjectsInfo, err error) {

	var (
		entries     []mongoEntry
		nextMarker  string
		parent      interface{}
		isRecursive = delimiter == ""
	)

	// query the DB
	parent = path.Join(bucket, prefix) + "/"
	if isRecursive {
		parent = bson.M{"$regex": fmt.Sprintf("^%s?", parent)}
	}

	findQuery := bson.M{
		"_id":    bson.M{"$gt": marker},
		"parent": parent,
	}
	err = mm.getCol(ses).Find(findQuery).
		Sort("_id").
		Limit(maxKeys).
		All(&entries)
	if err != nil {
		return
	}

	prefixToTrim := bucket + "/"
	for _, e := range entries {
		key := strings.TrimPrefix(e.Key(), prefixToTrim)
		if e.isDir() {
			if !isRecursive { // recursive list doesn't return the dir
				// directory must be ended with '/' to be shown properly
				// in the web UI
				result.Prefixes = append(result.Prefixes, key+"/")
			}
		} else {
			var md metatypes.Metadata

			err = mm.decodeFn(e.Metadata, &md)
			if err != nil {
				return
			}
			result.Objects = append(result.Objects,
				CreateObjectInfo(bucket, key, &md))
		}
		nextMarker = path.Base(key)
	}

	// set the marker
	if len(result.Objects)+len(result.Prefixes) >= maxKeys {
		result.NextMarker = nextMarker
		result.IsTruncated = true
	}
	return
}

// Close implements metastor.DB.Close interface
func (mm *MongoMetaStor) Close() error {
	mm.ses.Close()
	return nil
}

// parent get parent key of a key
func (mm *MongoMetaStor) parent(key string) string {
	return path.Dir(key)
}

// getCol returns mongodb collection from the given
// mongodb session and namespace
func (mm *MongoMetaStor) getCol(ses *mgo.Session) *mgo.Collection {
	return ses.DB(mm.dbName).C("metastor")
}

func (mm *MongoMetaStor) dropDB() {
	mm.ses.DB(mm.dbName).DropDatabase()
}

// getSession clone/copy session from main session,
// to be used by each operation
func (mm *MongoMetaStor) getSession() *mgo.Session {
	return mm.ses.Copy()
}

// checkCreateParent creates parents if not exist yet
func (mm *MongoMetaStor) checkCreateParent(key string, col *mgo.Collection) error {
	parent := mm.parent(key)
	if parent == "." {
		return nil
	}

	// find parent, return if already exist exist
	err := col.FindId(parent).One(nil)
	if err == nil {
		return nil
	}

	// create parent if not exist yet
	err = mm.set(col, []byte(parent), nil)
	if err != nil {
		return err
	}

	// create grand parent
	return mm.checkCreateParent(parent, col)
}

var (
	_ Storage = (*MongoMetaStor)(nil)
)
