# Estuary Autoretrieve Publisher

A standalone service that queries Estuary CIDs and publishes them to Autoretrieve. This is was pulled out from the main Estuary repository built by [Gabe](https://github.com/gmelodie) and [Elijah](https://github.com/elijaharita), to isolate and run it as a background job.

## Installation
### Create the DB connection .env file

```
DB_NAME=
DB_HOST=
DB_USER=
DB_PASS=
DB_PORT=
```

### Install run
```
// Install
go mod tidy
go mod download
go build -tags netgo -ldflags '-s -w' -o estuary-autoretrieve-publisher

// Run
./estuary-autoretrieve-publisher
```

### Optional flags
```
//  these are the default values
--indexer-advertisement-interval=15m 
--indexer-url=https://cid.contact/ 
--advertise_offline_autoretrieve=false
--autoretrieve-provider-batch-size
```