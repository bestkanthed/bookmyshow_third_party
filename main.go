package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
)

var (
	kafkaProducer sarama.SyncProducer
)

func init() {
	var err error

	// Config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Errors = true    // For sync producer this needs to be true
	config.Producer.Return.Successes = true // For sync producer this needs to be true

	// Connect to a Kafka broker running locally
	// TODO : get the connection string from config
	brokers := []string{"localhost:9092"}
	kafkaProducer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

}

func main() {
	router := gin.Default()
	v1 := router.Group("/v1/bookmyshow/reservation")
	{
		v1.POST("/", createReservation)

	}
	router.Run()
}

type SeatReservationDTO struct {
	CustomerId        uint      `json:"customer_id" `
	PaymentIdentifier uint      `json:"payment_identifier" `
	SeatId            uint      `json:"seat_id" `
	TheaterId         uint      `json:"theater_id" `
	ShowStartTime     time.Time `json:"show_start_time" gorm:"type:datetime"`
	ShowEndTime       time.Time `json:"show_end_time" gorm:"type:datetime"`
}

func createReservation(c *gin.Context) {
	var (
		reservationDTO SeatReservationDTO
		err            error
	)

	fmt.Println("In createReservation ")

	if err = c.ShouldBindJSON(&reservationDTO); err == nil {
		fmt.Printf("In createReservation  : %+v\n", reservationDTO)
		err = persistReservation(&reservationDTO)
		sendMessageToPerformBooking(&reservationDTO)
		//return OK
		c.JSON(http.StatusAccepted, gin.H{"status": "created"})
	}

	if err != nil {
		// some params not correct
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

}

// Send message to Booking service to enable the next stage in the workflow
func sendMessageToPerformBooking(reservationDTO *SeatReservationDTO) {
	log.Println("sending message to  kickstart booking for ", reservationDTO)

	bytes, err := json.Marshal(reservationDTO)
	if err != nil {
		log.Println("error sending message to Kafka ", err)
		return
	}

	// We are not setting a message key, which means that all messages will
	// be distributed randomly over the different partitions.
	msg := &sarama.ProducerMessage{
		Topic: "create_booking",
		Value: sarama.ByteEncoder(bytes),
	}

	partition, offset, err := kafkaProducer.SendMessage(msg)
	if err != nil {
		fmt.Printf("FAILED to publish message: %s\n", err)
	} else {
		fmt.Printf("message sent | partition(%d)/offset(%d)\n", partition, offset)
	}
}
