package conf

// 配置文件结构体

type LogTransfer struct {
	KafkaConfig `ini:"kafka"`
	ESConfig    `ini:"es"`
}

type KafkaConfig struct {
	Address string `ini:"address"`
	Topic   string `ini:"topic"`
}

type ESConfig struct {
	Address string `ini:"address"`
	ChanMaxSize int `ini:"chan_max_size"`
	GoNum int `ini:"go_num"`
}
