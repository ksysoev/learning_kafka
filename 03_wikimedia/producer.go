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
		Addr:      kafka.TCP("localhost:9094", "localhost:9095", "localhost:9096"),
		Topic:     "wikimedia_updates",
		Balancer:  &kafka.LeastBytes{},
		BatchSize: 10,
	}

	client := sse.NewClient("https://stream.wikimedia.org/v2/stream/recentchange")

	client.SubscribeRaw(func(msg *sse.Event) {
		var data map[string]any

		json.Unmarshal(msg.Data, &data)

		var key string
		key, ok := data["server_name"].(string)
		if !ok {
			key = "unknown"
		}

		go func() {
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
