package main

import (
	"ar-standalone/autoretrieve"
	"context"
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	DefaultIndexerURL            = "https://cid.contact"
	DefaultAdvertisementInterval = "15m"
)

func main() {

	indexAdvertisementInterval := *flag.String("indexer-advertisement-interval", DefaultAdvertisementInterval, "Index Advertisement")
	indexerUrl := *flag.String("indexer-url", DefaultIndexerURL, "Index Advertisement")
	advertiseOfflineAutoretrieves := *flag.Bool("advertise_offline_autoretrieve", false, "Index Advertise Offline")

	//	 default is 25000.
	autoretrieve.AutoretrieveProviderBatchSize = uint(*flag.Int("autoretrieve-provider-batch-size", 25000, "Autoretrieve Provider Batch Size"))

	//	 setup your DB first
	database, err := setupDB()
	if err != nil {
		panic("failed to connect database") // don't even try creating an instance
	}

	provider, err := autoretrieve.NewARProviderInstance(
		database,
		autoretrieve.NewARProviderParam{
			IndexAdvertisementInterval:    indexAdvertisementInterval,
			IndexerUrl:                    indexerUrl,
			AdvertiseOfflineAutoretrieves: advertiseOfflineAutoretrieves,
		})

	if err != nil {
		fmt.Errorf("failed to create autoretrieve provider: %v", err)
		return // don't even start the provider
	}

	provider.Provider.Run(context.Background())
}

func setupDB() (*gorm.DB, error) { // it's a pointer to a gorm.DB

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
	if err != nil {
		return nil, err
	}
	return DB, nil
}
