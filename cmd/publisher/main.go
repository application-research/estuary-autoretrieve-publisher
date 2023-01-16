package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	DefaultIndexerURL            = "https://cid.contact"
	DefaultAdvertisementInterval = "15m"
	DefaultBatchSize             = uint(25000)
)

func main() {

	app := &cli.App{
		Action: cmd,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "advertisement-interval",
				Value: DefaultAdvertisementInterval,
			},
			&cli.StringFlag{
				Name:  "indexer-url",
				Value: DefaultIndexerURL,
			},
			&cli.BoolFlag{
				Name:  "advertise-offline",
				Value: false,
			},
			&cli.UintFlag{
				Name:  "batch-size",
				Value: DefaultBatchSize,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func cmd(ctx *cli.Context) error {
	database, err := setupDB()
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}

	advertisementIntervalString := ctx.String("advertisement-interval")
	advertisementInterval, err := time.ParseDuration(advertisementIntervalString)
	if err != nil {
		log.Fatalf("Invalid advertisement interval: %s", advertisementIntervalString)
	}

	indexerURL := ctx.String("indexer-url")

	advertiseOffline := ctx.Bool("advertise-offline")

	batchSize := ctx.Uint("batch-size")

	provider, err := NewProvider(database, Config{
		AdvertisementInterval: advertisementInterval,
		IndexerURL:            indexerURL,
		AdvertiseOffline:      advertiseOffline,
		BatchSize:             batchSize,
	})
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}

	provider.Run(ctx.Context)

	return nil
}

func setupDB() (*gorm.DB, error) {

	viper.SetConfigFile(".env")
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read .env: %v", err)
	}

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
