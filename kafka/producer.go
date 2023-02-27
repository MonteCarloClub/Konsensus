package kafka

import (
	"github.com/MonteCarloClub/log"
	"github.com/Shopify/sarama"
)

type Producer struct {
	KafkaProducer sarama.SyncProducer
}

func (p *Producer) InitProducer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	syncProducer, err := sarama.NewSyncProducer([]string{"106.14.244.78:9092"}, config)
	if err != nil {
		log.Error("fail to init kafka producer", "err", err)
		return
	}
	p.KafkaProducer = syncProducer
	log.Info("kafka producer inited")
}

func(p *Producer) SendToKafka(message string) {
	producerMessage := &sarama.ProducerMessage{}
	producerMessage.Topic = KafkaTopic
	producerMessage.Value = sarama.StringEncoder(message)

	partition, offset, err := p.KafkaProducer.SendMessage(producerMessage)
	if err != nil {
		log.Error("fail to send message to kafka", "message", message, "err", err)
		return
	}
	log.Info("message sent", "message", message, "partition", partition, "offset", offset)
}