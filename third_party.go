package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	cluster "github.com/bsm/sarama-cluster"
)

var (
	signals chan os.Signal
)

func init() {

	// setup config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Group.Return.Notifications = true

	// specify Broker co-ordinates and topics of interest
	// should be done from config
	brokers := []string{"localhost:9092"}
	topics := []string{"create_booking"}

	// trap SIGINT to trigger a shutdown.
	signals = make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// connect, and register specifying the consumer group name
	consumer, err := cluster.NewConsumer(brokers, "booking-service", topics, config)
	if err != nil {
		panic(err)
	}

	// process errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// process notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	//start the listener thread
	go handleCreateBookingMessage(consumer)
}

func handleCreateBookingMessage(consumer *cluster.Consumer) {

	for {

		select {
		case partition, ok := <-consumer.Partitions():
			if !ok {
				panic("kafka consumer  : error getting partitions..")
			}

			// start a separate goroutine to consume messages
			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {

					var reservationDTO SeatReservationDTO
					if err := json.Unmarshal(msg.Value, &reservationDTO); err != nil {
						fmt.Println("unmarshalling error", err)
						consumer.MarkOffset(msg, "")
						continue
					}

					// TODO make actual booking with seller

					// update status in DB
					updateReservationStatus(&reservationDTO, BookingMade)

					fmt.Printf("processed create booking message %s-%d-%d-%s-%s\n",
						msg.Topic,
						msg.Partition,
						msg.Offset,
						msg.Key,
						msg.Value) // <- Actually process message here

					consumer.MarkOffset(msg, "") // Commit offset for this  message

				}
			}(partition)
		case <-signals:
			fmt.Println("consumer killed..")
			return
		}

	}
}
