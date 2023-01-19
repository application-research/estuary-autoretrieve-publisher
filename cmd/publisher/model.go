package main

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"gorm.io/gorm"
)

type ContentType int64

const (
	Unknown ContentType = iota
	File
	Directory
)

type Autoretrieve struct {
	gorm.Model

	Handle            string `gorm:"unique"`
	Token             string `gorm:"unique"`
	LastConnection    time.Time
	LastAdvertisement time.Time
	PubKey            string `gorm:"unique"`
	Addresses         string
}

func (autoretrieve *Autoretrieve) AddrInfo() (*peer.AddrInfo, error) {
	addrStrings := strings.Split(autoretrieve.Addresses, ",")

	pubKeyBytes, err := crypto.ConfigDecodeKey(autoretrieve.PubKey)
	if err != nil {
		return nil, err
	}
	pubKey, err := crypto.UnmarshalPublicKey(pubKeyBytes)
	if err != nil {
		return nil, err
	}
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return nil, err
	}

	var addrs []multiaddr.Multiaddr
	var invalidAddrStrings []string
	for _, addrString := range addrStrings {
		addr, err := multiaddr.NewMultiaddr(addrString)
		if err != nil {
			invalidAddrStrings = append(invalidAddrStrings, addrString)
			continue
		}
		addrs = append(addrs, addr)
	}
	if len(invalidAddrStrings) != 0 {
		return nil, fmt.Errorf("got invalid addresses: %#v", invalidAddrStrings)
	}

	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}

	return &addrInfo, nil
}

// A batch that has been published for a specific autoretrieve
type PublishedBatch struct {
	gorm.Model

	FirstContentID     uint64
	Count              uint64
	AutoretrieveHandle string
}

func (PublishedBatch) TableName() string { return "published_batches" }

type HeartbeatAutoretrieveResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
	AdvertiseInterval string         `json:"advertiseInterval"`
}

type AutoretrieveListResponse struct {
	Handle            string         `json:"handle"`
	LastConnection    time.Time      `json:"lastConnection"`
	LastAdvertisement time.Time      `json:"lastAdvertisement"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
}

type AutoretrieveInitResponse struct {
	Handle            string         `json:"handle"`
	Token             string         `json:"token"`
	LastConnection    time.Time      `json:"lastConnection"`
	AddrInfo          *peer.AddrInfo `json:"addrInfo"`
	AdvertiseInterval string         `json:"advertiseInterval"`
}

type DbCID struct {
	CID cid.Cid
}

func (dbc *DbCID) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("dbcids must get bytes")
	}

	if len(b) == 0 {
		return nil
	}

	c, err := cid.Cast(b)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}

func (dbc DbCID) Value() (driver.Value, error) {
	return dbc.CID.Bytes(), nil
}

func (dbc DbCID) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbc.CID.String())
}

func (dbc *DbCID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	c, err := cid.Decode(s)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}

type Object struct {
	ID         uint64 `gorm:"primarykey"`
	Cid        DbCID  `gorm:"index"`
	Size       int
	Reads      int
	LastAccess time.Time
}
