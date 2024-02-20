package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/r3labs/sse/v2"
	"github.com/segmentio/kafka-go"
)

func main() {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9094", "localhost:9095", "localhost:9096"),
		Topic:    "wikimedia_updates",
		Balancer: &kafka.LeastBytes{},
	}

	client := sse.NewClient("https://stream.wikimedia.org/v2/stream/recentchange")

	client.SubscribeRaw(func(msg *sse.Event) {
		// By design of kafka-go it do batching of messages underhood and block until batch is ready
		// So to avoid blocking we need to publish in goroutines
		// alternativly we would need to batch on our own and reduce the batch timeout
		go func() {
			var data map[string]any

			json.Unmarshal(msg.Data, &data)

			var key string
			key, ok := data["server_name"].(string)
			if !ok {
				key = "unknown"
			}

			err := w.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(key),
					Value: msg.Data,
				},
			)

			if err != nil {
				slog.Error("failed to write message:", err)
			}
		}()
	})

}
