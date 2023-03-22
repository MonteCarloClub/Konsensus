package network

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/MonteCarloClub/Konsensus/etcd"
	"github.com/MonteCarloClub/Konsensus/kafka"
	"github.com/MonteCarloClub/Konsensus/pbft"
	"github.com/MonteCarloClub/Konsensus/util"
	"github.com/MonteCarloClub/kchain-middleware/handler"
	"github.com/MonteCarloClub/log"

	"github.com/MonteCarloClub/Krypto/sm2"
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
					depositoryValue.Status = "1"
					log.Info("Signature verified!!!")
				} else {
					depositoryValue.Status = "-1"
					log.Error("Signature verification failed!!!")
					continue
				}
			} //else {
			//}

			voteCount := 0

			for _, port := range util.Config.FolowerPorts {
				msg := pbft.PrePrepareMsg{
					Data:         depositoryValue.Data,
					SignResult:   depositoryValue.Signature,
					CryptoMethod: depositoryValue.CryptoMethod,
					PubKey:       depositoryValue.PubKey,
				}
				jsonMsg, err := json.Marshal(msg)
				if err != nil {
					log.Error("fail to marshal preprepare message", "err", err)
					continue
				}
				buff := bytes.NewBuffer(jsonMsg)
				res, err := http.Post("http://127.0.0.1:10001/verify"+port, "application/json", buff)
				if err != nil {
					log.Error("fail to send preprepare message", "err", err)
					continue
				}
				log.Info("node" + port + " verification succeed, process to prepare phase")
				log.Info("===============prepare phase begins==================")

				var result map[string]interface{}
				json.NewDecoder(res.Body).Decode(&result)
				if result["status"] == "ok" {
					log.Info("node" + port + " votes aye")
					voteCount++
				} else {
					log.Warn("node" + port + " votes nay")
				}
			}

			if voteCount >= 667 {
				log.Info("===============commit phase begins==================")
				depositoryValue.Height = strconv.FormatInt(time.Now().Unix(), 10)
				depositoryValueJson, _ := json.Marshal(depositoryValue)
				s.Putter.PutToEtcdKv(string(msg.Value), string(depositoryValueJson))
			}
		} else {
			log.Error("fail to unmarshal depository value", "err", err)
		}
	}
}

func (s *Server) StopServer() {
	s.Consumer.KafkaConsumer.Close()
	close(s.Consumer.MessageChan)
}
