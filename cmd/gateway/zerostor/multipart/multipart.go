package multipart

import (
	"errors"
	"io"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

var (
	// ErrCreateUploadID returned when the manager failed to creates
	// upload ID. Usually because it can't generate random string
	// after some amount of retries.
	ErrCreateUploadID = errors.New("failed to create upload ID")
)

// Manager represents multipart upload manager which handle
// all the things related to multipart upload
type Manager interface {
	// Initialize multipart upload
	Init(bucket, object string, metadata map[string]string) (string, error)

	// UploadPart handles a single part upload
	UploadPart(bucket, object, uploadID, etag string, partID int, rd io.Reader) (minio.PartInfo, error)

	// Complete completes the multipart upload process.
	// It do these things:
	// - concantenates all parts and store it to non-temporary storage
	// - delete all parts from temporary storage and clean the metadata
	Complete(bucket, object, uploadID string, parts []minio.CompletePart) (*metatypes.Metadata, error)

	// Abort the multipart upload process
	// clean all temporary storage and metadata
	Abort(bucket, object, uploadID string) error

	// ListUpload returns all unfinished multipart upload.
	ListUpload(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error)

	// ListParts list PartInfo of all parts of an upload identified by the given upload ID.
	// The parts are sorted ascended by part number & last upload time.
	ListParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (minio.ListPartsInfo, error)

	// Close closes the manager
	Close() error
}

// MetaManager represent metadata manager which handles
// all metadata related to multipart upload.
type MetaManager interface {
	// InitUpload initializes all metadata required
	// to do multipart upload
	Init(bucket, object string, metadata map[string]string) (info Info, err error)

	// AddPart add PartInfo to an upload ID
	AddPart(bucket, uploadID string, partID int, info PartInfo) error

	// Remove PartInfo with given etag & partID from an uploadID
	DelPart(bucket, uploadID string, etag string, partID int) error

	// SetZstorMeta set 0-stor metadata of the multipart metadata to the given md
	SetZstorMeta(md metatypes.Metadata) error

	// ListUpload returns all unfinished multipart upload, sorted by upload ID
	ListUpload(bucket string) ([]Info, error)

	// GetMultipart returns Info all PartInfo for an uploadID
	// sorted ascended by partID
	GetMultipart(bucket, uploadID string) (Info, []PartInfo, error)

	// Clean cleans all metadata for an upload ID
	Clean(bucket, uploadID string) error
}

// Storage represents temporary storage used to store
// parts of multipart upload
type Storage interface {
	Write(bucket, object string, rd io.Reader, metadata map[string]string) (*metatypes.Metadata, error)
	Read(bucket, object string, writer io.Writer) error
	Delete(bucket, object string) error
}

// Info represents info/metadata of a multipart upload
type Info struct {
	minio.MultipartInfo
	Metadata map[string]string
}

// PartInfo represent info/metadata of an uploaded part
type PartInfo struct {
	minio.PartInfo
	Object    string // object name in 0-stor
	ZstorMeta metatypes.Metadata
}

// NewDefaultManager creates new default multipart manager
func NewDefaultManager(stor Storage, metaDir string, metaStor meta.Storage) (Manager, error) {
	metaMgr, err := newFilemetaUploadMgr(metaDir, metaStor)
	if err != nil {
		return nil, err
	}

	return NewManager(stor, metaMgr), nil
}

// NewManager creates new upload manager with given
// Storage and MetaManager
func NewManager(stor Storage, metaMgr MetaManager) Manager {
	return newManager(stor, metaMgr)
}
