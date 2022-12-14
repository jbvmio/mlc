package mlc

import (
	"log"

	"github.com/hashicorp/memberlist"
)

const (
	ProtocolVersion = 4
)

type NodeConfig struct {
	Name          string
	BindAddr      string
	AdvertiseAddr string
	DirLockPath   string
	BindPort      int
	AdvertisePort int
	Peers         []string
	Logger        *log.Logger
}

func (C *NodeConfig) SetDefaults() {
	if C.BindAddr == "" {
		C.BindAddr = "0.0.0.0"
	}
	if C.BindPort == 0 {
		C.BindPort = 7946
	}
	if C.AdvertisePort == 0 {
		C.AdvertisePort = C.BindPort
	}
}

func (C *NodeConfig) CreateNode() *Node {
	return CreateNode(C)
}

func CreateNode(config *NodeConfig) *Node {
	conf := memberlist.DefaultLANConfig()
	config.SetDefaults()
	if config.Logger == nil {
		config.Logger = log.Default()
	}
	conf.Logger = config.Logger
	conf.BindAddr = config.BindAddr
	conf.BindPort = config.BindPort
	conf.AdvertiseAddr = config.AdvertiseAddr
	conf.AdvertisePort = config.AdvertisePort
	conf.ProtocolVersion = ProtocolVersion
	if config.Name != "" {
		conf.Name = config.Name
	}
	return &Node{
		Config: conf,
		L:      config.Logger,
	}
}

type Node struct {
	Config      *memberlist.Config
	DirLockPath string
	L           *log.Logger
}
