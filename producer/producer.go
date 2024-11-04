package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// creating a new producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	defer producer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	count := 0

	for {
		select {
		case <-signals:
			return
		default:
			message := fmt.Sprintf("Message %d", count)
			msg := &sarama.ProducerMessage{
				Topic: "Message-topic",
				Value: sarama.StringEncoder(message),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("Failed to send message: %v\n", err)
			} else {
				log.Printf("Message sent: %s, Partition: %d, Offset: %d\n", message, partition, offset)
			}
			count++
		}
	}
}
