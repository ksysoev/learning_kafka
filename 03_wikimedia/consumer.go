package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9094", "localhost:9095", "localhost:9096"},
		GroupID:  "OpenSearchPublisher",
		Topic:    "wikimedia_updates",
		MinBytes: 1e3,  // 10KB
		MaxBytes: 10e6, // 10MB
	})

	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"http://localhost:9200"},
	})

	if err != nil {
		log.Fatal("Error creating OpenSearch client: ", err)
	}

	settings := strings.NewReader(`{
		"settings": {
		  "index": {
			   "number_of_shards": 1,
			   "number_of_replicas": 1
			   }
			 }
		}`)

	// Create an index with non-default settings.
	req := opensearchapi.IndicesCreateRequest{
		Index: "wikimedia_updates",
		Body:  settings,
	}

	_, err = req.Do(context.Background(), client)
	if err != nil {
		log.Fatal("Error creating index: ", err)
	}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message:", err)
			break
		}

		var data map[string]any

		json.Unmarshal(m.Value, &data)

		docId, ok := data["id"].(float64)

		if !ok {
			slog.Error("failed to get document id")
			continue
		}

		document := strings.NewReader(string(m.Value))
		req := opensearchapi.IndexRequest{
			Index:      "wikimedia_updates",
			DocumentID: fmt.Sprintf("%.0f", docId),
			Body:       document,
		}
		resp, err := req.Do(context.Background(), client)

		if err != nil {
			slog.Error("Error indexing document: ", err)
		} else {
			slog.Info("Indexing document: ", resp)
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
