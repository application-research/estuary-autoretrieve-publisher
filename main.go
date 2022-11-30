package main

import (
	"ar-standalone/autoretrieve"
	"flag"
)

func main() {

	indexAdvertisementInterval := *flag.String("indexer-advertisement-interval", "", "Index Advertisement")
	indexerUrl := *flag.String("indexer-url", "", "Index Advertisement")
	advertiseOfflineAutoretrieves := *flag.Bool("advertise_offline_autoretrieve", false, "Index Advertise Offline")

	autoretrieve.NewARProviderInstance(autoretrieve.NewARProviderParam{
		IndexAdvertisementInterval:    indexAdvertisementInterval,
		IndexerUrl:                    indexerUrl,
		AdvertiseOfflineAutoretrieves: advertiseOfflineAutoretrieves,
	})
}
