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
	if etag == "" {
		err = minio.InvalidETag{}
		return
	}
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
	// get all  part info of this upload ID
	storedInfos, err := m.getStoredInfo(uploadID)
	if err != nil {
		return nil, err
	}

	var (
		// pipe to connect `reader` that download from temporary
		// storage to `writer` that upload to permanent storage
		storRd, storWr = io.Pipe()
		errCh          = make(chan error, 1)
	)

	// read the data one by one and stream it to the writer
	go func() {
		defer storWr.Close()

		for _, part := range parts {
			info, ok := storedInfos[part.ETag]
			if !ok {
				errCh <- minio.InvalidPart{}
				return
			}

			// get data
			if err := m.stor.Read(MultipartBucket, info.Object, storWr); err != nil {
				errCh <- err
				return
			}

			// delete the temporary object
			if err := m.stor.Delete(MultipartBucket, info.Object); err != nil {
				// we don't return error here because we don't return error
				// on failed deletion.
				// Other tool should do garbage storage cleanup
				continue
			}
			delete(storedInfos, part.ETag)

			// we don't check the error here because
			// we want to keep delete next part.
			// another tools should do metadata cleanup
			m.metaMgr.DelPart(uploadID, part.ETag, part.PartNumber)
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

	// delete undeleted part
	for _, info := range storedInfos {
		if err = m.stor.Delete(MultipartBucket, info.Object); err == nil {
			// we don't return error here because we don't return error
			// on failed deletion.
			// Other tool should do garbage storage cleanup

			m.metaMgr.DelPart(uploadID, info.ETag, info.PartNumber)
			// we don't check the error here because
			// we want to keep delete next part.
			// another tools should do metadata cleanup
		}
	}

	// clean metadata
	return md, nil
}

// Abort implements Manager.Abort
func (m *manager) Abort(bucket, object, uploadID string) error {
	parts, err := m.metaMgr.ListPart(uploadID)
	if err != nil {
		return err
	}

	for _, part := range parts {
		if err := m.stor.Delete(MultipartBucket, part.Object); err != nil {
			// we don't return error because we want to delete next part.
			// another tool shoudl do storage cleanup
			continue
		}
		// we don't check the error here because
		// we want to keep delete next part.
		// another tools should do metadata cleanup
		m.metaMgr.DelPart(uploadID, part.ETag, part.PartNumber)
	}
	return nil
}

// Close implements Manager.Close
func (m *manager) Close() error {
	return nil
}

func (m *manager) getStoredInfo(uploadID string) (map[string]PartInfo, error) {
	infoArr, err := m.metaMgr.ListPart(uploadID)
	if err != nil {
		return nil, err
	}
	infos := make(map[string]PartInfo, len(infoArr))
	for _, info := range infoArr {
		infos[info.ETag] = info
	}
	return infos, nil
}

func (m *manager) objectName(uploadID string, partID int) string {
	return path.Join(uploadID) + "/" + fmt.Sprint(partID)
}

var (
	_ Manager = (*manager)(nil)
)
