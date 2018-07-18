package zerostor

import (
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/multipart"
)

func newMultipartManager(confFile, metaDir string, stor multipart.Storage, metaStor meta.Storage) (multipart.Manager, error) {
	cfg, err := readConfig(confFile)
	if err != nil {
		return nil, err
	}

	return newMultipartManagerFromCfg(cfg.Minio.MultipartMeta, metaDir, stor, metaStor)
}

func newMultipartManagerFromCfg(cfg metaCfg, metaDir string, stor multipart.Storage, metaStor meta.Storage) (multipart.Manager, error) {

	var (
		err error
		mgr multipart.Manager
	)

	switch cfg.Type {
	case metaTypeMongo:
		var metaMgr multipart.MetaManager
		metaMgr, err = multipart.NewMongoMetaMgr(cfg.Mongo.URL, cfg.Mongo.Database, metaStor)
		if err != nil {
			return nil, err
		}
		mgr = multipart.NewManager(stor, metaMgr)
	default:
		// we use filemeta by default
		mgr, err = multipart.NewDefaultManager(stor, metaDir, metaStor)
	}
	return mgr, err
}
