package util

import (
	"encoding/json"
	"fmt"
	"os"
)

type ConfigType struct {
	KafkaServer  []string `json:"kafka_server"`
	KafkaTopic   string   `json:"kafka_topic"`
	KafkaGroup   string   `json:"kafka_group"`
	KafkaOffset  int64    `json:"kafka_offset"`
	EtcdServer   []string `json:"etcd_server"`
	DialTimeout  int64    `json:"dial_timeout"`
	LeaderPort   string   `json:"leader_port"`
	FolowerPorts []string `json:"folloer_ports"`
}

var Config ConfigType

func ReadConfig() {
	file, err := os.Open("config.json")
	if err != nil {
		fmt.Println("open config file failed", err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&Config)
	if err != nil {
		fmt.Println("decode config file failed", err)
		return
	}

}

func WriteConfig() {
	file, err := os.OpenFile("config.json", os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("open config file failed", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(Config)
	if err != nil {
		fmt.Println("encode config file failed", err)
		return
	}
}
