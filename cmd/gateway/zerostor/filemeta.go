package zerostor

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

var (
	rootDirPerm  = os.FileMode(0755)
	fileMetaPerm = os.FileMode(0644)
)

// filemeta is metadata that implements
// github.com/zero-os/0-stor/client/metastor/db.DB interface
type filemeta struct {
	rootDir         string // meta root dir
	bucketDir       string // dir of buckets meta
	bktMgr          *bucketMgr
	objDir          string // dir of objects meta
	marshalFuncPair *encoding.MarshalFuncPair
	metaCli         *metastor.Client
}

func newFilemeta(rootDir string, marshalFuncPair *encoding.MarshalFuncPair) (*filemeta, error) {
	// initialize bucket's dir
	bucketDir := filepath.Join(rootDir, "buckets")
	if err := os.MkdirAll(bucketDir, rootDirPerm); err != nil {
		return nil, err
	}

	// initialize objDir
	objDir := filepath.Join(rootDir, "objects")
	if err := os.MkdirAll(objDir, rootDirPerm); err != nil {
		return nil, err
	}

	bktMgr, err := newBucketMgr(bucketDir, objDir)
	if err != nil {
		return nil, err
	}

	return &filemeta{
		rootDir:         rootDir,
		bucketDir:       bucketDir,
		objDir:          objDir,
		bktMgr:          bktMgr,
		marshalFuncPair: marshalFuncPair,
	}, nil
}

func (fm *filemeta) getBucketName(key []byte) string {
	elems := strings.SplitN(string(key), string(filepath.Separator), 2)
	return elems[0]
}

// Set implements Set interface
func (fm *filemeta) Set(namespace, key, metadata []byte) error {
	// check that the bucket exist
	bucket := fm.getBucketName(key)
	if _, ok := fm.bktMgr.get(bucket); !ok {
		return minio.BucketNotFound{}
	}

	return createWriteFile(fm.filename(key), metadata, fileMetaPerm)
}

func createWriteFile(filename string, content []byte, perm os.FileMode) error {
	// try to writes file directly
	err := ioutil.WriteFile(filename, content, perm)
	if !os.IsNotExist(err) {
		return err
	}

	// we don't check the dir existence before because
	// most of the time the dir will already exist
	if err := os.MkdirAll(filepath.Dir(filename), rootDirPerm); err != nil {
		return err
	}
	return ioutil.WriteFile(filename, content, perm)
}

func (fm *filemeta) Get(namespace, key []byte) (metadata []byte, err error) {
	metadata, err = ioutil.ReadFile(fm.filename(key))
	if err != nil && os.IsNotExist(err) {
		err = db.ErrNotFound
	}
	return
}

func (fm *filemeta) Delete(namespace, key []byte) error {
	err := os.Remove(fm.filename(key))
	if err != nil && os.IsNotExist(err) {
		err = db.ErrNotFound
	}
	return err
}

// TODO : provide a kind of protection
func (fm *filemeta) Update(namespace, key []byte, cb db.UpdateCallback) error {
	mdIn, err := fm.Get(namespace, key)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	mdOut, err := cb(mdIn)
	if err != nil {
		return err
	}

	return fm.Set(namespace, key, mdOut)
}

func (fm *filemeta) Close() error {
	return nil
}

func (fm *filemeta) ListKeys(namespace []byte, cb db.ListCallback) error {
	return nil
}

func (fm *filemeta) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	dir := filepath.Join(fm.objDir, bucket, prefix)

	// check, it is a file or dir
	fi, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = db.ErrNotFound
		}
		return
	}

	// if dir, list the dir
	if fi.Mode().IsDir() {
		return fm.listDir(bucket, prefix)
	}

	// if file, get metadata of this file
	md, err := fm.metaCli.GetMetadata([]byte(prefix))
	if err != nil {
		return
	}

	result.Objects = append(result.Objects, createObjectInfo(bucket, prefix, md))
	return
}

func (fm *filemeta) listDir(bucket, dir string) (result minio.ListObjectsInfo, err error) {
	files, dirs, err := fm.readDir(bucket, dir)
	if err != nil {
		return
	}

	result.Prefixes = dirs
	for _, f := range files {
		var md *metatypes.Metadata

		key := filepath.Join(bucket, dir, f)

		md, err = fm.metaCli.GetMetadata([]byte(key))
		if err != nil {
			return
		}
		result.Objects = append(result.Objects,
			createObjectInfo(bucket, filepath.Join(dir, f), md))
	}
	return
}

func (fm *filemeta) readDir(bucket, dir string) (files []string, dirs []string, err error) {
	absDir := filepath.Join(fm.objDir, bucket, dir)
	fios, err := ioutil.ReadDir(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			err = db.ErrNotFound
		}
		return
	}

	for _, f := range fios {
		if f.IsDir() {
			dirs = append(dirs, f.Name())
		} else {
			files = append(files, f.Name())
		}
	}
	return
}

func (fm *filemeta) filename(key []byte) string {
	return filepath.Join(fm.objDir, string(key))
}
