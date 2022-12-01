package autoretrieve

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"time"
)

type ARInstance struct {
	DB       *gorm.DB
	Provider *Provider
}

type NewARProviderParam struct {
	IndexAdvertisementInterval    string
	IndexerUrl                    string
	AdvertiseOfflineAutoretrieves bool
}

var (
	AutoretrieveProviderBatchSize = uint(25000)
)

func NewARProviderInstance(DB *gorm.DB, param NewARProviderParam) (*ARInstance, error) {
	var arInstance *ARInstance
	duration, err := time.ParseDuration(param.IndexAdvertisementInterval)
	if err != nil {
		panic(err)
	}

	arProvider, err := NewProvider(
		DB,
		duration,
		param.IndexerUrl,
		param.AdvertiseOfflineAutoretrieves,
	)

	if err != nil {
		fmt.Errorf("failed to create autoretrieve provider instance: %v", err)
		return nil, err
	}

	arInstance = &ARInstance{
		DB:       DB,
		Provider: arProvider,
	}
	return arInstance, nil
}

func (ar *ARInstance) Run() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Errorf("Autoretrieve provide loop panicked, cancelling until the executable is restarted: %v", err)
			}
		}()

		if err := ar.Provider.Run(context.Background()); err != nil {
			fmt.Errorf("Autoretrieve provide loop failed, cancelling until the executable is restarted: %v", err)
		}
	}()
	defer ar.Provider.Stop()
}
