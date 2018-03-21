package multipart

import (
	"errors"
	"io"

	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
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
	Init(bucket, object string) (string, error)

	// UploadPart handles a single part upload
	UploadPart(bucket, object, uploadID, etag string, partID int, rd io.Reader) (minio.PartInfo, error)

	// Complete completes the multipart upload process.
	// It do these things:
	// - concantenates all parts and store it to non-temporary storage
	// - delete all parts from temporary storage and clean the metadata
	Complete(bucket, object, uploadID string, parts []minio.CompletePart) error

	Abort(bucket, object, uploadID string) error
}

// MetaManager represent metadata manager which handles
// all metadata related to multipart upload.
type MetaManager interface {
	// InitUpload initializes all metadata required
	// to do multipart upload
	Init(bucket, object string) (uploadID string, err error)

	// AddPart add PartInfo to an upload ID
	AddPart(uploadID string, partID int, info PartInfo) error

	// ListPart returns all PartInfo for an uploadID
	// sorted ascended by partID
	ListPart(uploadID string) (map[int]PartInfo, error)

	// Clean cleans all metadata for an upload ID
	Clean(uploadID string) error
}

// Storage represents temporary storage used to store
// parts of multipart upload
type Storage interface {
	Write(bucket, object string, rd io.Reader) (*metatypes.Metadata, error)
	Read(bucket, object string, writer io.Writer, offset, length int64) error
}

// PartInfo represent info/metadata of an uploaded part
type PartInfo struct {
	ZstorKey string
	minio.PartInfo
}

// NewManager creates new default multipart manager
func NewManager(stor Storage, metaMgr MetaManager) Manager {
	return &zstorMultipartMgr{
		stor: stor,
		mm:   metaMgr,
	}
}
