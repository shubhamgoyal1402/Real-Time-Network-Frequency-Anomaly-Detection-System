package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func produceMessages(brokerList []string, topic string) {
	producer, err := sarama.NewSyncProducer(brokerList, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	for {
		// Simulate network frequency between 47.0 and 53.0 Hz (with normal range around 49.5-50.5)
		frequency := 49.0 + rand.Float64()*4.0
		message := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(strconv.FormatFloat(frequency, 'f', 2, 64)),
		}

		// Send the message to Kafka
		partition, offset, err := producer.SendMessage(&message)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			log.Printf("Sent frequency: %.2f to partition %d at offset %d\n", frequency, partition, offset)
		}

		// Sleep for 0.25 second to simulate real-time traffic
		time.Sleep(250 * time.Millisecond)
	}
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "network_frequency"

	log.Println("Starting network frequency simulator...")
	produceMessages(brokers, topic)
}
