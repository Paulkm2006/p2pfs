// Package model defines versioned control-plane records shared by clients and server.
package model

const DataShards = 4
const ParityShards = 2
const TotalShards = 6
const MaxFileSize int64 = 1 << 30

type Shard struct {
	Index  int    `json:"index"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type Manifest struct {
	ID           string  `json:"id"`
	Format       int     `json:"format"`
	Name         string  `json:"name"`
	Size         int64   `json:"size"`
	SHA256       string  `json:"sha256"`
	DataShards   int     `json:"data_shards"`
	ParityShards int     `json:"parity_shards"`
	Codec        string  `json:"codec"`
	Shards       []Shard `json:"shards"`
}
type Identity struct {
	UUID         string `json:"uuid"`
	DeviceSecret string `json:"device_secret"`
}
type RegisterRequest struct {
	Identity
	Name string `json:"name"`
}
type RegisterResponse struct {
	Identity
	Token            string      `json:"token"`
	ICEServers       []ICEServer `json:"ice_servers"`
	HeartbeatSeconds int         `json:"heartbeat_seconds"`
}
type ICEServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}
type HoldingsRequest struct {
	FileID  string `json:"file_id"`
	Indices []int  `json:"indices"`
}
type Source struct {
	PeerID   string `json:"peer_id"`
	Indices  []int  `json:"indices"`
	LastSeen int64  `json:"last_seen"`
}
type FileInfo struct {
	Manifest Manifest `json:"manifest"`
	Sources  []Source `json:"sources"`
}
type Signal struct {
	ID    string `json:"id"`
	From  string `json:"from"`
	To    string `json:"to"`
	Type  string `json:"type"`
	SDP   string `json:"sdp"`
	Relay bool   `json:"relay"`
}
