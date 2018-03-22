package multipart

import (
	"fmt"
	"io"
	"path"

	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

const (
	// MultipartBucket is bucket in 0-stor server used as temporary
	// storage for multipart upload part.
	// Client couldn't create bucket with this name
	MultipartBucket = ".multipart"
)

type manager struct {
	metaMgr MetaManager
	stor    Storage
}

func newManager(stor Storage, metaMgr MetaManager) *manager {
	return &manager{
		stor:    stor,
		metaMgr: metaMgr,
	}
}

// Init implements Manager.Init
func (m *manager) Init(bucket, object string) (string, error) {
	return m.metaMgr.Init(bucket, object)
}

// UploadPart implements Manager.UploadPart
func (m *manager) UploadPart(bucket, object, uploadID, etag string, partID int, rd io.Reader) (info minio.PartInfo, err error) {
	partObject := m.objectName(uploadID, partID)

	// stor the part to 0-stor server as temporary object
	md, err := m.stor.Write(MultipartBucket, partObject, rd)
	if err != nil {
		return
	}

	// minio part info
	info = minio.PartInfo{
		PartNumber:   partID,
		LastModified: minio.UTCNow(),
		Size:         md.Size,
		ETag:         etag,
	}

	// meta part info
	metaInfo := PartInfo{
		Object:   partObject,
		PartInfo: info,
	}
	err = m.metaMgr.AddPart(uploadID, partID, metaInfo)
	return
}

// Complete implements Manager.Complete
func (m *manager) Complete(bucket, object, uploadID string, parts []minio.CompletePart) (*metatypes.Metadata, error) {
	storedInfos, err := m.metaMgr.ListPart(uploadID)
	if err != nil {
		return nil, err
	}

	storRd, storWr := io.Pipe()
	errCh := make(chan error, 1)

	// read the data one by one and stream it to the writer
	go func() {
		defer storWr.Close()

		for _, part := range parts {
			info, ok := func() (PartInfo, bool) {
				for _, si := range storedInfos {
					if si.PartNumber == part.PartNumber && si.ETag == part.ETag {
						return si, true
					}
				}
				return PartInfo{}, false
			}()
			if !ok {
				errCh <- nil
				return
			}

			// get data
			if err := m.stor.Read(MultipartBucket, info.Object, storWr); err != nil {
				errCh <- err
				return
			}

			// delete the temporary object
			if err := m.stor.Delete(MultipartBucket, info.Object); err != nil {
				errCh <- err
				return
			}

		}
		errCh <- nil
	}()

	// write from the pipe
	md, err := m.stor.Write(bucket, object, storRd)
	if err != nil {
		return nil, err
	}

	// check error from the reader
	err = <-errCh
	if err != nil {
		return nil, err
	}

	// clean metadata
	return md, m.metaMgr.Clean(uploadID)
}

// Abort implements Manager.Abort
func (m *manager) Abort(bucket, object, uploadID string) error {
	parts, err := m.metaMgr.ListPart(uploadID)
	if err != nil {
		return err
	}

	for _, part := range parts {
		if err := m.stor.Delete(MultipartBucket, part.Object); err != nil {
			return err
		}

	}
	return m.metaMgr.Clean(uploadID)
}

func (m *manager) Close() error {
	return nil
}

func (m *manager) objectName(uploadID string, partID int) string {
	return path.Join(uploadID) + "/" + fmt.Sprint(partID)
}

var (
	_ Manager = (*manager)(nil)
)
