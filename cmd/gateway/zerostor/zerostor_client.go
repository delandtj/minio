package zerostor

import (
	"io"

	"github.com/threefoldtech/0-stor/client/datastor/pipeline/storage"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

// zstorClient define 0-stor client
type zstorClient interface {
	WriteWithUserMeta(key []byte, r io.Reader, userDefMeta map[string]string) (*metatypes.Metadata, error)
	Read(meta metatypes.Metadata, w io.Writer) error
	ReadRange(meta metatypes.Metadata, w io.Writer, offset, length int64) error
	Delete(md metatypes.Metadata) error
	Check(meta metatypes.Metadata, fast bool) (storage.CheckStatus, error)
	Repair(meta metatypes.Metadata) (*metatypes.Metadata, error)
	Close() error
}
