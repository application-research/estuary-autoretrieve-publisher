package main

import (
	"ar-standalone/autoretrieve"
	"flag"
	"fmt"
)

func main() {

	indexAdvertisementInterval := *flag.String("indexer-advertisement-interval", "", "Index Advertisement")
	indexerUrl := *flag.String("indexer-url", "", "Index Advertisement")
	advertiseOfflineAutoretrieves := *flag.Bool("advertise_offline_autoretrieve", false, "Index Advertise Offline")

	provider, err := autoretrieve.NewARProviderInstance(autoretrieve.NewARProviderParam{
		IndexAdvertisementInterval:    indexAdvertisementInterval,
		IndexerUrl:                    indexerUrl,
		AdvertiseOfflineAutoretrieves: advertiseOfflineAutoretrieves,
	})

	if err != nil {
		fmt.Errorf("failed to create autoretrieve provider: %v", err)
		return // don't even start the provider
	}

	provider.Run() // run it!!!
}
