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

func NewARProviderInstance(DB *gorm.DB, param NewARProviderParam) (*ARInstance, error) {

	duration, err := time.ParseDuration(param.IndexAdvertisementInterval)
	if err != nil {
		panic(err)
	}

	provider, err := NewProvider(
		DB,
		duration,
		param.IndexerUrl,
		param.AdvertiseOfflineAutoretrieves,
	)

	if err != nil {
		fmt.Errorf("failed to create autoretrieve provider instance: %v", err)
		return nil, err
	}

	return &ARInstance{
		DB:       DB,
		Provider: provider,
	}, nil
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
