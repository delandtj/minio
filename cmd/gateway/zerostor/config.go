package zerostor

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

const (
	metaTypeFile  = "file"
	metaTypeMongo = "mongo"
)

// minioZstorCfg defines non 0-stor configuration
// of minio 0-stor gateway.
type minioZstorCfg struct {
	ZerostorMeta zstorMetaCfg `yaml:"zerostor_meta"`
}

// zstorMetaCfg defines configuration of minio 0-stor gateway metadata
type zstorMetaCfg struct {
	Type  string       `yaml:"type"`
	Mongo mongoMetaCfg `yaml:"mongo"`
}

// mongoMetaCfg defines configuration of mongodb backed metadata
type mongoMetaCfg struct {
	URL      string `yaml:"url"`
	Database string `yaml:"database"`
}

func readConfig(confFile string) (*minioZstorCfg, error) {
	b, err := ioutil.ReadFile(confFile)
	if err != nil {
		return nil, err
	}

	// for now we only support YAML
	var cfg minioZstorCfg
	err = yaml.Unmarshal(b, &cfg)
	return &cfg, err
}
