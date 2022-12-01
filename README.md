# Estuary AR Service - Standalone version.

This is a decouple/standalone version of AR service / provider on Estuary built by (CTTO):
- https://github.com/elijaharita 
- https://github.com/gmelodie

The intention is to isolate this service and run it as a standalone background job.

## Installation
# Create the DB connection .env file

```
DB_NAME=
DB_HOST=
DB_USER=
DB_PASS=
DB_PORT=
```

# Install run
```
go build -tags netgo -ldflags '-s -w' -o ar-provider-run
./ar-provider-run
```
