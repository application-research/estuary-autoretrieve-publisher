package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multihash"

	providerpkg "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"gorm.io/gorm"
)

type Config struct {
	IndexerURL            string
	AdvertisementInterval time.Duration
	AdvertiseOffline      bool
	BatchSize             uint64
}

type Provider struct {
	engine *engine.Engine
	db     *gorm.DB
	cfg    Config
}

type Iterator struct {
	mhs           []multihash.Multihash
	index         uint64
	firstObjectID uint64
	count         uint64
}

func NewIterator(db *gorm.DB, firstObjectID uint64, count uint64) (*Iterator, error) {

	// Read CID strings for this object ID
	var cidStrings []string
	if err := db.Raw(
		"SELECT cid FROM objects WHERE id BETWEEN ? AND ?",
		firstObjectID,
		firstObjectID+count-1,
	).Scan(&cidStrings).Error; err != nil {
		return nil, err
	}

	log.Infof(
		"Creating iterator for object IDs %d to %d (%d MHs)",
		firstObjectID,
		firstObjectID+count,
		len(cidStrings),
	)

	// Parse CID strings and extract multihashes
	var mhs []multihash.Multihash
	// NOTE(@elijaharita 2022-12-11): CIDs are often empty in the database for
	// some reason, so I just count the amount that are empty and put one print
	// statement for them at the end to avoid thousands of lines of log spam.
	emptyCount := 0
	for _, cidString := range cidStrings {
		if cidString == "" {
			emptyCount++
			continue
		}

		_, cid, err := cid.CidFromBytes([]byte(cidString))
		if err != nil {
			log.Warnf("Failed to parse CID string '%s': %v", cidString, err)
			continue
		}

		mhs = append(mhs, cid.Hash())
	}

	if len(mhs) == 0 {
		return nil, fmt.Errorf("no multihashes")
	}

	if emptyCount != 0 {
		log.Warnf("Skipped %d empty CIDs", emptyCount)
	}

	return &Iterator{
		mhs:           mhs,
		firstObjectID: firstObjectID,
		count:         count,
	}, nil
}

func (iter *Iterator) Next() (multihash.Multihash, error) {
	if iter.index == uint64(len(iter.mhs)) {
		return nil, io.EOF
	}

	mh := iter.mhs[iter.index]

	iter.index++

	return mh, nil
}

func NewProvider(db *gorm.DB, ds datastore.Batching, cfg Config) (*Provider, error) {
	eng, err := engine.New(
		engine.WithPublisherKind(engine.DataTransferPublisher),
		engine.WithDirectAnnounce(cfg.IndexerURL),
		engine.WithDatastore(ds),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init engine: %v", err)
	}

	eng.RegisterMultihashLister(func(
		ctx context.Context,
		peer peer.ID,
		contextID []byte,
	) (providerpkg.MultihashIterator, error) {
		log := log.Named("lister")

		params, err := readContextID(contextID)
		if err != nil {
			return nil, err
		}

		log = log.With(
			"first_object_id", params.firstObjectID,
			"count", params.count,
			"indexer_peer_id", params.provider,
		)

		log.Infof(
			"Received pull request (peer ID: %s, first object ID: %d, count: %d)",
			params.provider,
			params.firstObjectID,
			params.count,
		)
		iter, err := NewIterator(db, params.firstObjectID, params.count)
		if err != nil {
			return nil, err
		}

		return iter, nil
	})

	return &Provider{
		engine: eng,
		db:     db,
		cfg:    cfg,
	}, nil
}

func (provider *Provider) Run(ctx context.Context) error {
	log := log.Named("loop")

	if err := provider.engine.Start(ctx); err != nil {
		return err
	}

	// time.Tick will drop ticks to make up for slow advertisements
	log.Infof("Starting autoretrieve advertisement loop every %s", provider.cfg.AdvertisementInterval)
	ticker := time.NewTicker(provider.cfg.AdvertisementInterval)
	for ; true; <-ticker.C {
		if ctx.Err() != nil {
			ticker.Stop()
			break
		}

		log.Infof("Starting autoretrieve advertisement tick")

		// Find the highest current object ID for later
		var lastObject Object
		if err := provider.db.Last(&lastObject).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Infof("Failed to get last provider object ID: %v", err)
				continue
			} else {
				log.Warnf("No objects to advertise")
				continue
			}
		}

		var autoretrieves []Autoretrieve
		if err := provider.db.Find(&autoretrieves).Error; err != nil {
			log.Errorf("Failed to get autoretrieves: %v", err)
			continue
		}

		// For each registered autoretrieve...
		for _, autoretrieve := range autoretrieves {
			log := log.With("autoretrieve_handle", autoretrieve.Handle)

			// Make sure it is online (if offline checking isn't disabled)
			if !provider.cfg.AdvertiseOffline {
				if time.Since(autoretrieve.LastConnection) > provider.cfg.AdvertisementInterval {
					log.Debugf("Skipping offline autoretrieve")
					continue
				}
			}

			// Get address info for later
			addrInfo, err := autoretrieve.AddrInfo()
			if err != nil {
				log.Errorf("Failed to get autoretrieve address info: %v", err)
				continue
			}

			// For each batch that should be advertised...
			for firstObjectID := uint64(0); firstObjectID <= lastObject.ID; firstObjectID += provider.cfg.BatchSize {

				// Find the amount of objects in this batch (likely less than
				// the batch size if this is the last batch)
				count := provider.cfg.BatchSize
				remaining := lastObject.ID - firstObjectID
				if remaining < count {
					count = remaining
				}

				log := log.With("first_object_id", firstObjectID, "count", count)

				// Search for an entry (this array will have either 0 or 1
				// elements depending on whether an advertisement was found)
				var publishedBatches []PublishedBatch
				if err := provider.db.Where(
					"autoretrieve_handle = ? AND first_content_id = ?",
					autoretrieve.Handle,
					firstObjectID,
				).Find(&publishedBatches).Error; err != nil {
					log.Errorf("Failed to get published objects: %v", err)
					continue
				}

				// And check if it's...

				// 1. fully advertised, or no changes: do nothing
				if len(publishedBatches) != 0 && uint64(publishedBatches[0].Count) == count {
					log.Debugf("Skipping already advertised batch")
					continue
				}

				// The batch size should always be the same unless the
				// config changes
				contextID, err := makeContextID(contextParams{
					provider:      addrInfo.ID,
					firstObjectID: firstObjectID,
					count:         provider.cfg.BatchSize,
				})
				if err != nil {
					log.Errorf("Failed to make context ID: %v", err)
					continue
				}

				// 2. not advertised: notify put, create DB entry, continue
				if len(publishedBatches) == 0 {
					adCid, err := provider.engine.NotifyPut(
						ctx,
						addrInfo,
						contextID,
						metadata.Default.New(metadata.Bitswap{}),
					)
					if err != nil {
						// If there was an error, check whether already
						// advertised
						if errors.Is(err, providerpkg.ErrAlreadyAdvertised) {
							// If so, try deleting it first...
							log.Warnf("Batch was unexpectedly already advertised, removing old batch")
							if _, err := provider.engine.NotifyRemove(ctx, addrInfo.ID, contextID); err != nil {
								log.Errorf("Failed to remove unexpected existing advertisement: %v", err)
							}

							// ...and then re-advertise
							_adCid, err := provider.engine.NotifyPut(
								ctx,
								addrInfo,
								contextID,
								metadata.Default.New(metadata.Bitswap{}),
							)
							if err != nil {
								log.Errorf("Failed to publish batch after deleting unexpected existing advertisement: %v", err)
								continue
							}

							adCid = _adCid
						} else {
							// Otherwise, fail out
							log.Errorf("Failed to publish batch: %v", err)
							continue
						}
					}

					log.Infof("Published new batch with advertisement CID %s", adCid)
					if err := provider.db.Create(&PublishedBatch{
						FirstContentID:     firstObjectID,
						AutoretrieveHandle: autoretrieve.Handle,
						Count:              count,
					}).Error; err != nil {
						log.Errorf("Failed to write batch to database: %v", err)
					}
					continue
				}

				// 3. incompletely advertised: delete and then notify put,
				// update DB entry, continue
				publishedBatch := publishedBatches[0]
				if publishedBatch.Count != count {
					oldAdCid, err := provider.engine.NotifyRemove(
						ctx,
						addrInfo.ID,
						contextID,
					)
					if err != nil {
						log.Warnf("Failed to remove batch (going to re-publish anyway): %v", err)
					}
					log.Infof("Removed old advertisement")

					adCid, err := provider.engine.NotifyPut(
						ctx,
						addrInfo,
						contextID,
						metadata.Default.New(metadata.Bitswap{}),
					)
					if err != nil {
						log.Errorf("Failed to publish batch: %v", err)
						continue
					}

					log.Infof("Updated incomplete batch with new ad CID %s (previously %s)", adCid, oldAdCid)
					publishedBatch.Count = count
					if err := provider.db.Save(&publishedBatch).Error; err != nil {
						log.Errorf("Failed to update batch in database")
					}
					continue
				}
			}
		}
	}

	return nil
}

func (provider *Provider) Stop() error {
	return provider.engine.Shutdown()
}

type contextParams struct {
	provider      peer.ID
	firstObjectID uint64
	count         uint64
}

// Object ID to context ID
func makeContextID(params contextParams) ([]byte, error) {
	contextID := make([]byte, 16)
	binary.BigEndian.PutUint64(contextID[0:8], params.firstObjectID)
	binary.BigEndian.PutUint64(contextID[8:16], params.count)

	peerIDBytes, err := params.provider.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to write context peer ID: %v", err)
	}
	contextID = append(contextID, peerIDBytes...)
	return contextID, nil
}

// Context ID to object ID
func readContextID(contextID []byte) (contextParams, error) {
	peerID, err := peer.IDFromBytes(contextID[16:])
	if err != nil {
		return contextParams{}, fmt.Errorf("failed to read context peer ID: %v", err)
	}

	return contextParams{
		provider:      peerID,
		firstObjectID: binary.BigEndian.Uint64(contextID[0:8]),
		count:         binary.BigEndian.Uint64(contextID[8:16]),
	}, nil
}
