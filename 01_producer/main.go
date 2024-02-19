package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

func main() {
	w := &kafka.Writer{
		Addr:      kafka.TCP("localhost:9094", "localhost:9095", "localhost:9096"),
		Topic:     "test",
		Balancer:  &kafka.LeastBytes{},
		BatchSize: 10,
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("Writing message", i)
			err := w.WriteMessages(context.Background(),
				kafka.Message{
					Value: []byte(fmt.Sprintf("message 0 %d", i)),
				},
				kafka.Message{
					Value: []byte(fmt.Sprintf("message 1 %d", i)),
				},
				kafka.Message{
					Value: []byte(fmt.Sprintf("message 2 %d", i)),
				},
			)

			if err != nil {
				log.Fatal("failed to write messages:", err)
			}
		}()
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
