package kafka

import (
	"sync"
	"sync/atomic"

	"github.com/MonteCarloClub/Konsensus/util"
	"github.com/MonteCarloClub/log"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	KafkaConsumer sarama.Consumer
	// client        sarama.Client
	// offsetManager sarama.OffsetManager
	MessageChan chan *sarama.ConsumerMessage
}

// InitConsumer 初始化kafka消费者
func (c *Consumer) InitConsumer() {
	// 初始化kafka消费者
	consumer, err := sarama.NewConsumer(util.Config.KafkaServer, nil)
	if err != nil {
		log.Error("fail to initiate kafka consumer", "err", err)
		return
	}
	c.KafkaConsumer = consumer

	// // 初始化kafka client和offset manager
	// c.client, err = sarama.NewClient(util.Config.KafkaServer, nil)
	// if err != nil {
	// 	log.Error("fail to initiate kafka client", "err", err)
	// 	return
	// }
	// c.offsetManager, err = sarama.NewOffsetManagerFromClient(util.Config.KafkaGroup, c.client)
	// if err != nil {
	// 	log.Error("fail to initiate kafka offset manager", "err", err)
	// 	return
	// }

	// 初始化kafka消息channel
	c.MessageChan = make(chan *sarama.ConsumerMessage, 1000)
	log.Info("kafka consumer initiated")
}

// ReceiveFromKafka 从kafka接收消息
func (c *Consumer) ReceiveFromKafka() {

	// 获取kafka分区
	partitions, err := c.KafkaConsumer.Partitions(util.Config.KafkaTopic)
	if err != nil {
		log.Error("fail to get kafka partitions", "err", err)
		return
	}
	log.Info("kafka partitions got", "partitions", partitions)

	// partitionOffsetManager, err := c.offsetManager.ManagePartition(util.Config.KafkaTopic, 0)
	// if err != nil {
	// 	log.Error("fail to get partition offset manager", "err", err)
	// 	return
	// }
	// defer partitionOffsetManager.Close()
	// defer c.offsetManager.Close()
	// defer c.client.Close()

	var wg sync.WaitGroup

	// 遍历分区
	for partition := range partitions {

		// 获取分区offset
		// offset, _ := partitionOffsetManager.NextOffset()
		// log.Info("kafka partition offset got", "partition", partition, "offset", offset)

		// OffsetNewest: 即时消费, OffsetOldest: 从积压的开始
		partitionConsumer, err := c.KafkaConsumer.ConsumePartition((util.Config.KafkaTopic), int32(partition), util.Config.KafkaOffset)
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
				// kafka消息写入channel
				c.MessageChan <- msg
				log.Info("kafka message received", "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "value", string(msg.Value))
				atomic.AddInt64(&util.Config.KafkaOffset, 1)
			}
		}(partitionConsumer)
	}

	wg.Wait()
}
