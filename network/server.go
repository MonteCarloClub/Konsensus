package network

import (
	"encoding/json"
	"time"

	"github.com/MonteCarloClub/kchain-middleware/handler"
	"github.com/MonteCarloClub/log"
	"github.com/yunxiaozhao/Konsensus/etcd"
	"github.com/yunxiaozhao/Konsensus/kafka"
	"github.com/yunxiaozhao/Konsensus/util"

	"github.com/MonteCarloClub/Krypto/sm2"
)

type Server struct {
	Consumer kafka.Consumer
	Putter   etcd.Putter
}

func (s *Server) StartServer() {
	//初始化配置
	util.ReadConfig()

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
		if data == "" {
			continue
		}
		if err := json.Unmarshal([]byte(data), &depositoryValue); err == nil {
			if depositoryValue.CryptoMethod == "sm" {
				log.Info("sm2 signature verification", "TxHash", string(msg.Value))
				pubKey, err := sm2.RawBytesToPublicKey([]byte(depositoryValue.PubKey))
				if err != nil {
					log.Error("fail to convert public key", "err", err)
					continue
				}
				if sm2.Verify(pubKey, nil, []byte(depositoryValue.Data), []byte(depositoryValue.Signature)) {
					log.Info("Signature verified!!!")
				} else {
					log.Error("Signature verification failed!!!")
					continue
				}
			} //else {
			//}
			depositoryValue.Status = "1"
			depositoryValue.Height = time.Now().GoString()
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
