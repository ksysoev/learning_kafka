package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	w := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9094", "localhost:9095", "localhost:9096"),
		Topic: "test",
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Value: []byte("One!"),
		},
		kafka.Message{
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
