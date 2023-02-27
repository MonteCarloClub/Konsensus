package network

import (
	"encoding/json"
	"fmt"

	"github.com/MonteCarloClub/kchain-middleware/handler"
	"github.com/Shopify/sarama"
	"github.com/yunxiaozhao/Konsensus/etcd"
	"github.com/yunxiaozhao/Konsensus/kafka"
)

type Server struct {
	Consumer kafka.Consumer
	Producer kafka.Producer
	Putter   etcd.Putter
}

func (s *Server) StartServer() {
	s.Consumer.InitConsumer()
	s.Producer.InitProducer()
	s.Putter.InitEtcdClient()
	go s.Consumer.ReceiveFromKafka()
	var msg *sarama.ConsumerMessage
	for {
		select {
		case msg = <-s.Consumer.MessageChan:
			var depositoryValue handler.DepositoryValue
			fmt.Print("msg.Value: ", string(msg.Value), "\n")
			if len(string(msg.Value)) < 2 || string(msg.Value)[0:2] != "0x" {
				continue
			}
			data := s.Putter.GetFromEtcdKv(string(msg.Value))
			fmt.Printf("#####%v\n", data)
			if err := json.Unmarshal([]byte(data), &depositoryValue); err == nil {
				depositoryValue.Status = "1"
				depositoryValueJson, _ := json.Marshal(depositoryValue)
				s.Putter.PutToEtcdKv(string(msg.Value), string(depositoryValueJson))
			} else {
				fmt.Println(err)
			}

			s.Producer.SendToKafka(string(msg.Value))
		}
	}
}

func (s *Server) StopServer() {
	s.Consumer.KafkaConsumer.Close()
	s.Producer.KafkaProducer.Close()
}
