package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9094", "localhost:9095", "localhost:9096"},
		GroupID:  "my-group",
		Topic:    "test",
		MinBytes: 1e3,  // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message:", err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s, Partition: %v\n", m.Offset, string(m.Key), string(m.Value), m.Partition)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}
