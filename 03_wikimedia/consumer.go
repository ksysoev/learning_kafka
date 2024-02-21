package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"http://localhost:9200"},
	})

	if err != nil {
		log.Fatal("Error creating OpenSearch client: ", err)
	}

	if err := createIndexIfNotExists(client, "wikimedia"); err != nil {
		log.Fatal("Error creating index: ", err)
	}

	consumer := Consumer{
		ready:    make(chan bool),
		OSClient: client,
	}

	keepRunning := true

	ctx, cancel := context.WithCancel(context.Background())
	group, err := sarama.NewConsumerGroup([]string{"localhost:9094", "localhost:9095", "localhost:9096"}, "OSPublisher", config)
	if err != nil {
		log.Fatal("Error creating consumer group: ", err)
	}
	defer func() { _ = group.Close() }()

	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := group.Consume(ctx, []string{"wikimedia_updates"}, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(group, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err = group.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func createIndexIfNotExists(client *opensearch.Client, index string) error {
	reqExists := opensearchapi.IndicesExistsRequest{
		Index: []string{index},
	}

	resExists, err := reqExists.Do(context.Background(), client)
	if err != nil {
		return err
	}

	if resExists.StatusCode == 200 {
		return nil
	}

	settings := `{
		"settings": {
		  "index": {
			   "number_of_shards": 1,
			   "number_of_replicas": 0
			   }
			 }
		}`

	reqCreate := opensearchapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(settings),
	}

	resCreate, err := reqCreate.Do(context.Background(), client)
	if err != nil {
		return err
	}

	if resCreate.StatusCode != 200 {
		return fmt.Errorf("Error creating index: %s", resCreate.String())
	}

	return nil
}

type Consumer struct {
	ready    chan bool
	OSClient *opensearch.Client
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			id := extractID(message.Value)
			if id == "" {
				log.Println("Error extracting ID from message")
				continue
			}

			document := strings.NewReader(string(message.Value))
			req := opensearchapi.IndexRequest{
				Index:      "wikimedia",
				Body:       document,
				Refresh:    "true",
				DocumentID: id,
			}
			resp, err := req.Do(context.Background(), consumer.OSClient)

			if err != nil {
				slog.Error("Error indexing document: ", err)
			} else {
				slog.Info("Indexing document: ", resp)
			}
			resp.Body.Close()

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

func extractID(data []byte) string {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return ""
	}

	metadata, ok := m["meta"].(map[string]interface{})
	if !ok {
		return ""
	}

	id, ok := metadata["id"]

	if !ok {
		return ""
	}

	return id.(string)
}
