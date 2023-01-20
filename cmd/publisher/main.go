package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"net/http"
	_ "net/http/pprof"

	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	DefaultIndexerURL            = "https://cid.contact"
	DefaultAdvertisementInterval = "15m"
	DefaultBatchSize             = uint64(25000)
)

func main() {
	app := &cli.App{
		Action: cmd,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "advertisement-interval",
				Value:   DefaultAdvertisementInterval,
				EnvVars: []string{"AR_PUB_ADVERTISEMENT_INTERVAL"},
			},
			&cli.StringFlag{
				Name:    "indexer-url",
				Value:   DefaultIndexerURL,
				EnvVars: []string{"AR_PUB_INDEXER_URL"},
			},
			&cli.BoolFlag{
				Name:    "advertise-offline",
				Value:   false,
				EnvVars: []string{"AR_PUB_ADVERTISE_OFFLINE"},
			},
			&cli.Uint64Flag{
				Name:    "batch-size",
				Value:   DefaultBatchSize,
				EnvVars: []string{"AR_PUB_BATCH_SIZE"},
			},
			&cli.StringFlag{
				Name:    "data-dir",
				Value:   "data",
				EnvVars: []string{"AR_PUB_DATA_DIR"},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func cmd(ctx *cli.Context) error {
	go func() {
		http.ListenAndServe("0.0.0.0:8080", nil)
	}()

	database, err := setupDB()
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}

	dataDir := ctx.String("data-dir")

	ds, err := leveldb.NewDatastore(path.Join(dataDir, "datastore"), nil)
	if err != nil {
		return err
	}

	advertisementIntervalString := ctx.String("advertisement-interval")
	advertisementInterval, err := time.ParseDuration(advertisementIntervalString)
	if err != nil {
		log.Fatalf("Invalid advertisement interval: %s", advertisementIntervalString)
	}

	indexerURL := ctx.String("indexer-url")

	advertiseOffline := ctx.Bool("advertise-offline")

	batchSize := ctx.Uint64("batch-size")

	provider, err := NewProvider(database, ds, Config{
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
