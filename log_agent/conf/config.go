package conf

type AppConfig struct {
	KafkaConfig `ini:"kafka"`
	EtcdConfig `ini:"etcd"`
	LocalConfig `ini:"local"`
}

type KafkaConfig struct {
	Address string `ini:"address"`
	ChanMaxSize int `ini:"chan_max_size"`
}

type EtcdConfig struct {
	Address string `ini:"address"`
	Key string `ini:"collect_log_ley"`
	Timeout int `ini:"timeout"`
}

type LocalConfig struct {
	Ip string `ini:"ip"`
}

// ----unused----
type TailLogConfig struct {
	Filename string `ini:"filename"`
}
