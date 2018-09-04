package meta

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	minio "github.com/minio/minio/cmd"
	"github.com/threefoldtech/0-stor/client/metastor/db"
	"github.com/threefoldtech/0-stor/client/metastor/encoding"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

var (
	dirPerm  = os.FileMode(0755)
	filePerm = os.FileMode(0644)
)
var (
	errMaxKeyReached = errors.New("max keys reached")
)

const (
	metaObjectDir   = "objects"
	fileMetaDirSize = 4096 // size of dir always 4096
)

// filemeta is file based metadata implementation that implements
// metaStorage interface
type filemeta struct {
	rootDir      string // meta root dir
	objDir       string // dir of objects meta
	lock         sync.RWMutex
	readDirCache *readDirCache
	encodeFunc   encoding.MarshalMetadata
	decodeFunc   encoding.UnmarshalMetadata
}

// NewDefaultMetastor creates default Storage implementation.
// The implementation is using file based storage
func NewDefaultMetastor(rootDir string, marshalFuncPair *encoding.MarshalFuncPair) (Storage, error) {
	// initialize objDir
	objDir := filepath.Join(rootDir, metaObjectDir)
	if err := os.MkdirAll(objDir, dirPerm); err != nil {
		return nil, err
	}

	rdc, err := newReadDirCache()
	if err != nil {
		return nil, err
	}

	return &filemeta{
		rootDir:      rootDir,
		objDir:       objDir,
		readDirCache: rdc,
		encodeFunc:   marshalFuncPair.Marshal,
		decodeFunc:   marshalFuncPair.Unmarshal,
	}, nil
}

// Set implements Set interface
func (fm *filemeta) Set(namespace, key, metadata []byte) error {
	fm.lock.Lock()

	err := fm.set(namespace, key, metadata)

	fm.lock.Unlock()

	return err
}
func (fm *filemeta) set(namespace, key, metadata []byte) error {
	return createWriteFile(fm.filename(key), metadata, filePerm)
}

// Get implements 0-stor/client/metastor/db.DB.Get
// It handles two kind of meta:
// - file : returns content of file
// - directory: creates meta based on the dir info
func (fm *filemeta) Get(namespace, key []byte) (metadata []byte, err error) {
	fm.lock.RLock()
	md, err := fm.get(namespace, key)
	fm.lock.RUnlock()
	return md, err
}

func (fm *filemeta) get(namespace, key []byte) (metadata []byte, err error) {
	fileDirName := fm.filename(key) // it could be dir or file

	fi, err := os.Stat(fileDirName)
	if err != nil {
		if os.IsNotExist(err) {
			err = db.ErrNotFound
		}
		return nil, err
	}

	if !fi.IsDir() {
		return ioutil.ReadFile(fileDirName)
	}

	epoch := fi.ModTime().UnixNano()
	return fm.encodeFunc(metatypes.Metadata{
		Namespace:      namespace,
		Size:           fileMetaDirSize,
		Key:            key,
		StorageSize:    fileMetaDirSize,
		CreationEpoch:  epoch,
		LastWriteEpoch: epoch,
	})
}

func (fm *filemeta) Delete(namespace, key []byte) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	err := os.Remove(fm.filename(key))
	if err != nil && os.IsNotExist(err) {
		err = db.ErrNotFound
	}
	return err
}

// TODO : provide a kind of protection
func (fm *filemeta) Update(namespace, key []byte, cb db.UpdateCallback) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	mdIn, err := fm.get(namespace, key)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	mdOut, err := cb(mdIn)
	if err != nil {
		return err
	}

	return fm.set(namespace, key, mdOut)
}

// get the metadata and decode it
func (fm *filemeta) GetDecodeMeta(key []byte) (*metatypes.Metadata, error) {
	fm.lock.RLock()
	defer fm.lock.RUnlock()

	rawMd, err := fm.get([]byte(""), key)
	if err != nil {
		return nil, err
	}

	var md metatypes.Metadata

	err = fm.decodeFunc(rawMd, &md)
	return &md, err
}

func (fm *filemeta) Encode(md metatypes.Metadata) ([]byte, error) {
	return fm.encodeFunc(md)
}

func (fm *filemeta) Close() error {
	return nil
}

func (fm *filemeta) ListKeys(namespace []byte, cb db.ListCallback) error {
	return nil
}

// handle minio.ObjectLayer.ListObjects
func (fm *filemeta) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	dir := filepath.Join(fm.objDir, bucket, prefix)

	fm.lock.RLock()
	defer fm.lock.RUnlock()

	// check, it is a file or dir
	fi, err := os.Stat(dir)
	if err != nil {
		if prefix == "" { // if root bucket, no need to return error
			err = nil
			return
		}
		if os.IsNotExist(err) {
			err = db.ErrNotFound
		}
		return
	}

	// if dir, list the dir
	if fi.Mode().IsDir() {
		// when requesting for a dir, the prefix must
		// ended with trailing slash
		if prefix != "" && !strings.HasSuffix(prefix, "/") && delimiter != "" {
			result.Prefixes = []string{prefix + "/"}
			return
		}
		return fm.listDir(bucket, prefix, marker, delimiter, maxKeys)
	}

	// if file, get metadata of this file
	md, err := fm.GetDecodeMeta([]byte(filepath.Join(bucket, prefix)))
	if err != nil {
		return
	}

	result.Objects = append(result.Objects, CreateObjectInfo(bucket, prefix, md))
	return
}

func (fm *filemeta) listDir(bucket, dir, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	files, dirs, nextMarker, err := fm.readDir(bucket, dir, marker, delimiter, maxKeys)
	if err != nil {
		return
	}

	for _, d := range dirs {
		result.Prefixes = append(result.Prefixes,
			filepath.Join(dir, d)+"/") // directory dirent must ended with "/" to be shown properly in web UI
	}

	for _, f := range files {
		var md *metatypes.Metadata

		key := filepath.Join(bucket, dir, f)

		md, err = fm.GetDecodeMeta([]byte(key))
		if err != nil {
			return
		}
		result.Objects = append(result.Objects,
			CreateObjectInfo(bucket, filepath.Join(dir, f), md))
	}

	// set the marker
	if len(result.Objects)+len(result.Prefixes) >= maxKeys {
		result.NextMarker = filepath.Join(dir, nextMarker)
		result.IsTruncated = true
	}

	return
}

func (fm *filemeta) readDir(bucket, dir, marker, delimiter string, maxKeys int) (files []string, dirs []string, nextMarker string, err error) {
	absDir := filepath.Join(fm.objDir, bucket, dir)

	if delimiter == "" { // recursive list
		files, nextMarker, err = fm.readDirRecursive(absDir, marker, maxKeys)
		return
	}

	names, err := fm.readDirCache.GetNames(absDir)
	if err != nil {
		return
	}

	var (
		afterMarker = marker == ""
		numKeys     int
	)
	marker = filepath.Base(marker)

	for _, name := range names {
		if numKeys == maxKeys {
			return
		}
		if afterMarker == false {
			if name == marker {
				afterMarker = true
			}
			continue
		}
		numKeys++

		var isDir bool

		isDir, err = isDirectory(absDir, name)
		if err != nil {
			return
		}
		if isDir {
			dirs = append(dirs, name)
		} else {
			files = append(files, name)
		}
		nextMarker = name
	}
	return
}

// TODO: consider to  put this operation in the readDirCache
func isDirectory(dir, name string) (bool, error) {
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return false, err
	}

	return info.IsDir(), nil
}

func (fm *filemeta) readDirRecursive(absDir, marker string, maxKeys int) (files []string, nextMarker string, err error) {
	var (
		prefixToTrim = absDir
		numKeys      int
		afterMarker  = marker == ""
	)
	if !strings.HasSuffix(prefixToTrim, "/") {
		prefixToTrim = prefixToTrim + "/"
	}

	err = filepath.Walk(absDir, func(path string, fi os.FileInfo, err error) error {
		// stop the walk when getting err != nil
		if err != nil {
			return err
		}

		// stop the walk when reach the max keys
		if numKeys == maxKeys {
			return errMaxKeyReached
		}

		// ignore the dir
		if fi.IsDir() {
			return nil
		}

		// trim the path
		relPath := strings.TrimPrefix(path, prefixToTrim)

		// ignore entries before the `marker`
		if afterMarker == false {
			if relPath == marker {
				afterMarker = true
			}
			return nil
		}

		numKeys++

		files = append(files, relPath)

		nextMarker = relPath
		return nil
	})
	if err == errMaxKeyReached {
		err = nil
	}
	return
}

func (fm *filemeta) getBucketName(key []byte) string {
	elems := strings.SplitN(string(key), string(filepath.Separator), 2)
	return elems[0]
}

func createWriteFile(filename string, content []byte, perm os.FileMode) error {
	// try to writes file directly
	err := ioutil.WriteFile(filename, content, perm)
	if !os.IsNotExist(err) {
		return err
	}

	// we don't check the dir existence before because
	// most of the time the dir will already exist
	if err := os.MkdirAll(filepath.Dir(filename), dirPerm); err != nil {
		return err
	}
	return ioutil.WriteFile(filename, content, perm)
}

func (fm *filemeta) filename(key []byte) string {
	return filepath.Join(fm.objDir, string(key))
}

var (
	_ Storage = (*filemeta)(nil)
)
