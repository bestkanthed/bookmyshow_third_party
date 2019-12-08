package main

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type Status int

const (
	Initial     Status = 0
	BookingMade Status = 1
	EmailSent   Status = 2
)

type SeatReservation struct {
	//SeatReservationDetails `gorm:"embedded"`
	CustomerId        uint      `json:"customer_id" `
	PaymentIdentifier uint      `json:"payment_identifier" `
	SeatId            uint      `json:"seat_id" `
	TheaterId         uint      `json:"theater_id" `
	ShowStartTime     time.Time `json:"check_in" gorm:"type:datetime"`
	ShowEndTime       time.Time `json:"check_out" gorm:"type:datetime"`
	Id                string
	Status            Status
}

type AvailabilityThreshold struct {
	gorm.Model
	SeatId       uint
	TheaterId    uint
	Availability int
}

var (
	db *gorm.DB
)

func init() {
	var err error
	db, err = gorm.Open("mysql", "root:@tcp(127.0.0.1:3306)/bookmyshow?charset=utf8&parseTime=True")
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema
	//db.AutoMigrate(&SeatReservation{})
	db.AutoMigrate(&AvailabilityThreshold{})

	// dummy thresholds
	db.Create(&AvailabilityThreshold{SeatId: 1, TheaterId: 2, Availability: 3})

}

// generates ID for the reservation from SeatId, TheaterId and show start time
func makeId(res *SeatReservationDTO) string {
	// NOTE : for uniqueness, non-overlapping reservations, there should be another explicit check
	return fmt.Sprintf("%v#%v#%v", res.SeatId, res.TheaterId, res.ShowStartTime)
}

func persistReservation(res *SeatReservationDTO) error {
	// Note the use of tx as the database handle once you are within a transaction
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if tx.Error != nil {
		return tx.Error
	}

	//TODO : Check that there is no overlapping reservation

	if err := tx.Create(&SeatReservation{
		CustomerId:        res.CustomerId,
		PaymentIdentifier: res.PaymentIdentifier,
		SeatId:            res.SeatId,
		TheaterId:         res.TheaterId,
		ShowStartTime:     time.Time(res.ShowStartTime),
		ShowEndTime:       time.Time(res.ShowEndTime),
		Id:                makeId(res),
		Status:            Initial}).Error; err != nil {
		tx.Rollback()
		return err
	}

	fmt.Println("created reservation..")

	// update the entry for availability threshold
	var threshold AvailabilityThreshold
	tx.Where("seat_id = ? AND theater_id = ?", res.SeatId, res.TheaterId).First(&threshold)

	fmt.Printf("\nthreshold = %+v\n", threshold)
	tx.Model(&threshold).Where("id = ?", threshold.ID).Update("availability", threshold.Availability-1)

	// NOTE : availability is just a threshold for update here.
	// Even if availability is 0, reservation is forwarded to the Seller
	// And availability >0 in thresholds DB is not a guarantee of reservation certainty.
	if threshold.Availability <= 1 {
		// we have reached threshold
		sendInvalidationMessageToPriceStore(threshold.SeatId, threshold.TheaterId)

	}

	return tx.Commit().Error
}

func sendInvalidationMessageToPriceStore(eid, rid uint) {
	fmt.Println("sending message to invalid catalog for seat id ", eid, " theater id ", rid)
}

func updateReservationStatus(reservationDTO *SeatReservationDTO, status Status) error {
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if tx.Error != nil {
		return tx.Error
	}

	tx.Model(&SeatReservation{}).Where("id = ?", makeId(reservationDTO)).Update("status", status)

	return tx.Commit().Error
}
