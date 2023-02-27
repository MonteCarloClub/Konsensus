package network

import (
	"encoding/json"

	"github.com/MonteCarloClub/kchain-middleware/handler"
	"github.com/MonteCarloClub/log"
	"github.com/yunxiaozhao/Konsensus/etcd"
	"github.com/yunxiaozhao/Konsensus/kafka"
)

type Server struct {
	Consumer kafka.Consumer
	Putter   etcd.Putter
}

func (s *Server) StartServer() {

	//初始化kafka消费者和etcd修改器
	s.Consumer.InitConsumer()
	s.Putter.InitEtcdClient()

	go s.Consumer.ReceiveFromKafka()
	for msg := range s.Consumer.MessageChan {
		var depositoryValue handler.DepositoryValue
		log.Info("receive message from kafka", "value", string(msg.Value))
		if len(string(msg.Value)) < 2 || string(msg.Value)[0:2] != "0x" {
			continue
		}
		data := s.Putter.GetFromEtcdKv(string(msg.Value))
		if err := json.Unmarshal([]byte(data), &depositoryValue); err == nil {
			depositoryValue.Status = "1"
			depositoryValueJson, _ := json.Marshal(depositoryValue)
			s.Putter.PutToEtcdKv(string(msg.Value), string(depositoryValueJson))
		} else {
			log.Error("fail to unmarshal depository value", "err", err)
		}
	}
}

func (s *Server) StopServer() {
	s.Consumer.KafkaConsumer.Close()
	close(s.Consumer.MessageChan)
}
