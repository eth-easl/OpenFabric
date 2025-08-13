package cmd

type P2PConfig struct {
	Port string `json:"port" yaml:"port"`
}

type VaccumConfig struct {
	Interval int `json:"interval" yaml:"interval"`
}

type QueueConfig struct {
	Port string `json:"port" yaml:"port"`
}

type Config struct {
	Path    string        `json:"path" yaml:"path"`
	Port    string        `json:"port" yaml:"port"`
	Name    string        `json:"name" yaml:"name"`
	P2p     P2PConfig     `json:"p2p" yaml:"p2p"`
	Vacuum  VaccumConfig  `json:"vacuum" yaml:"vacuum"`
	Queue   QueueConfig   `json:"queue" yaml:"queue"`
	Account AccountConfig `json:"account" yaml:"account"`
	Seed    string        `json:"seed" yaml:"seed"`
	TCPPort string        `json:"tcp_port" yaml:"tcp_port"`
	UDPPort string        `json:"udp_port" yaml:"udp_port"`
}

type AccountConfig struct {
	Wallet string `json:"wallet" yaml:"wallet"`
}

var defaultConfig = Config{
	Seed:    "0",
	Path:    "",
	Port:    "8092",
	Name:    "relay",
	TCPPort: "43905",
	UDPPort: "59820",
	P2p:     P2PConfig{Port: "8093"},
	Vacuum:  VaccumConfig{Interval: 10},
	Queue:   QueueConfig{Port: "8094"},
	Account: AccountConfig{Wallet: ""},
}