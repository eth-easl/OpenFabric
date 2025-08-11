package protocol

import (
	"context"
	"crypto/rand"
	mrand "math/rand"
	"ocf/internal/common"
	"strconv"
	"sync"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dualdht "github.com/libp2p/go-libp2p-kad-dht/dual"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/spf13/viper"
)

var P2PNode *host.Host
var ddht *dualdht.DHT
var hostOnce sync.Once
var MyID string

func GetP2PNode(ds datastore.Batching) (host.Host, dualdht.DHT) {
	hostOnce.Do(func() {
		ctx := context.Background()
		var err error
		seed := viper.GetString("seed")
		// try to parse the seed as int64
		seedInt, err := strconv.ParseInt(seed, 10, 64)
		if err != nil {
			panic(err)
		}
		host, err := newHost(ctx, seedInt, ds)
		MyID = host.ID().String()
		P2PNode = &host
		if err != nil {
			panic(err)
		}
	})
	return *P2PNode, *ddht
}

func newHost(ctx context.Context, seed int64, ds datastore.Batching) (host.Host, error) {
	// connmgr, err := connmgr.NewConnManager(
	// 	50,
	// 	500,
	// 	connmgr.WithGracePeriod(time.Minute),
	// )
	// if err != nil {
	// 	common.Logger.Error("Error while creating connection manager: %v", err)
	// }
	var err error
	var priv crypto.PrivKey
	// try to load the private key from file
	if seed == 0 {
		// try to load from the file
		priv = loadKeyFromFile()
		if priv == nil {
			r := rand.Reader
			priv, _, err = crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
			if err != nil {
				return nil, err
			}
			writeKeyToFile(priv)
		}
	} else {
		r := mrand.New(mrand.NewSource(seed))
		priv, _, err = crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
		if err != nil {
			return nil, err
		}
		writeKeyToFile(priv)
	}

	if err != nil {
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.DefaultTransports,
		libp2p.Identity(priv),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		// libp2p.ConnectionManager(connmgr),
		libp2p.NATPortMap(),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/"+viper.GetString("tcpport"),
			"/ip4/0.0.0.0/tcp/"+viper.GetString("tcpport")+"/ws",
			"/ip4/0.0.0.0/udp/"+viper.GetString("udpport")+"/quic",
		),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.Security(noise.ID, noise.New),
		libp2p.EnableNATService(),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.ForceReachabilityPublic(),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			ddht, err = newDHT(ctx, h, ds)
			return ddht, err
		}),
	}
	return libp2p.New(opts...)
}

func newDHT(ctx context.Context, h host.Host, ds datastore.Batching) (*dualdht.DHT, error) {
	dhtOpts := []dualdht.Option{
		dualdht.DHTOption(dht.NamespacedValidator("pk", record.PublicKeyValidator{})),
		dualdht.DHTOption(dht.NamespacedValidator("ipns", ipns.Validator{KeyBook: h.Peerstore()})),
		dualdht.DHTOption(dht.Concurrency(512)),
		dualdht.DHTOption(dht.Mode(dht.ModeAuto)),
	}
	if ds != nil {
		dhtOpts = append(dhtOpts, dualdht.DHTOption(dht.Datastore(ds)))
	}
	return dualdht.New(ctx, h, dhtOpts...)
}

// GetConnectedPeers returns the list of connected peers
func ConnectedPeers() []*peer.AddrInfo {
	var pinfos []*peer.AddrInfo = []*peer.AddrInfo{}
	host, _ := GetP2PNode(nil)
	for _, p := range host.Peerstore().Peers() {
		// check if the peer is connected
		if host.Network().Connectedness(p) == network.Connected {
			pinfos = append(pinfos, &peer.AddrInfo{
				ID:    p,
				Addrs: host.Peerstore().Addrs(p),
			})
		}
	}
	return pinfos
}

func AllPeers() []*PeerWithStatus {
	var pinfos []*PeerWithStatus = []*PeerWithStatus{}
	host, _ := GetP2PNode(nil)
	for _, p := range host.Peerstore().Peers() {
		pinfos = append(pinfos, &PeerWithStatus{
			ID:            p.String(),
			Connectedness: host.Network().Connectedness(p).String(),
		})
	}
	return pinfos
}

func ConnectedBootstraps() []string {
	var bootstraps = []string{}
	dnt := GetNodeTable(false)
	host, _ := GetP2PNode(nil)
	for _, p := range *dnt {
		if p.PublicAddress != "" {
			common.Logger.Info("Peer: ", p.ID, " Public Address: ", p.PublicAddress, " Connectedness: ", host.Network().Connectedness(peer.ID(p.ID)), " Host ID: ", host.ID())
			if host.Network().Connectedness(peer.ID(p.ID)) == network.Connected || host.ID().String() == p.ID {
				bootstrapAddr := "/ip4/" + p.PublicAddress + "/tcp/" + viper.GetString("tcpport") + "/p2p/" + p.ID
				bootstraps = append(bootstraps, bootstrapAddr)
			}
		}
	}
	return bootstraps
}
