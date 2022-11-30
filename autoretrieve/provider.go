package autoretrieve

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
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

func NewARProviderInstance(param NewARProviderParam) (*ARInstance, error) {

	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()

	dbHost, okHost := viper.Get("DB_HOST").(string)
	dbUser, okUser := viper.Get("DB_USER").(string)
	dbPass, okPass := viper.Get("DB_PASS").(string)
	dbName, okName := viper.Get("DB_NAME").(string)
	dbPort, okPort := viper.Get("DB_PORT").(string)
	if !okHost || !okUser || !okPass || !okName || !okPort {
		panic("invalid database configuration")
	}

	dsn := "host=" + dbHost + " user=" + dbUser + " password=" + dbPass + " dbname=" + dbName + " port=" + dbPort + " sslmode=disable TimeZone=Asia/Shanghai"

	DB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

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
