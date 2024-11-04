package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
    return nil
}

func (ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
    return nil
}

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error { // Line 21
    for message := range claim.Messages() {
        fmt.Printf("Received message: %s\n", string(message.Value))
        session.MarkMessage(message, "")
    }
    return nil
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "consumer-group", config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Println("Start consuming messages...")
	ctx := context.Background()
	handler := ConsumerGroupHandler{}

	for {
		select {
		case <-signals:
			return
		default:
			if err := consumerGroup.Consume(ctx, []string{"example-topic"}, handler); err != nil {
				log.Fatalf("Error during consumption: %v", err)
			}
		}
	}
}
