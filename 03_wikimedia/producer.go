package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/r3labs/sse/v2"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 250 * time.Millisecond

	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 20 * time.Millisecond
	config.Producer.Flush.Bytes = 32 * 1024

	brokers := []string{"localhost:9094", "localhost:9095", "localhost:9096"}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Panic(err)
	}

	client := sse.NewClient("https://stream.wikimedia.org/v2/stream/recentchange")

	client.SubscribeRaw(func(msg *sse.Event) {
		var data map[string]interface{}

		json.Unmarshal(msg.Data, &data)

		var key string
		key, ok := data["server_name"].(string)
		if !ok {
			key = "unknown"
		}

		message := &sarama.ProducerMessage{
			Topic: "wikimedia_updates",
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(msg.Data),
		}
		go func() {
			select {
			case producer.Input() <- message:
			case err := <-producer.Errors():
				log.Println("Failed to produce message", err)
			}
		}()
	})
}
