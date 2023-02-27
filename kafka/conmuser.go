package kafka

import (
	"fmt"
	"github.com/MonteCarloClub/log"
	"github.com/Shopify/sarama"
	"sync"
)

type Consumer struct {
	KafkaConsumer sarama.Consumer
	MessageChan   chan *sarama.ConsumerMessage
}

func (c *Consumer) InitConsumer() {
	consumer, err := sarama.NewConsumer([]string{"106.14.244.78:9092"}, nil)
	if err != nil {
		log.Error("fail to init kafka consumer", "err", err)
		return
	}
	c.KafkaConsumer = consumer
	c.MessageChan = make(chan *sarama.ConsumerMessage, 1000)
	log.Info("kafka consumer inited")
}

func (c *Consumer) ReceiveFromKafka() {
	partitions, err := c.KafkaConsumer.Partitions(KafkaTopic)
	if err != nil {
		log.Error("fail to get kafka partitions", "err", err)
		return
	}
	log.Info("kafka partitions got", "partitions", partitions)

	var wg sync.WaitGroup
	for partition := range partitions {
		// OffsetNewest: 即时消费, OffsetOldest: 从积压的开始
		partitionConsumer, err := c.KafkaConsumer.ConsumePartition(KafkaTopic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			log.Error("fail to create partition consumer", "err", err)
			return
		}
		log.Info("kafka partition consumer created", "partition", partition)
		defer partitionConsumer.AsyncClose()

		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range partitionConsumer.Messages() {
				// todo: 实际业务发起，写入channel即可
				c.MessageChan <- msg
				fmt.Printf("partition: %v, offset: %v, key:%v, value:%v\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
		}(partitionConsumer)
	}
	wg.Wait()
}

