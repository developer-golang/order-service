package main

import (
	"command/database"
	"command/kafka"
	"command/migration"
	"command/order"
	"context"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

func main() {
	var dbconnection database.Config
	dbconnection.DBUser = "karthi"
	dbconnection.DBPassword = "Postgres"
	dbconnection.DBPort = 5432
	dbconnection.DBHost = "localhost"
	dbconnection.DBName = "postgres"
	err := database.InitDb(&dbconnection)
	if err != nil {
		log.Fatal(err)
		return
	}

	migration.PostgresMigration(database.Db)

	producer, err := kafka.NewProducer()
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	order.SetProducer(producer)

	consumer, err := kafka.NewConsumer()
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go kafka.ConsumePaymentCompleted(ctx, consumer, func(ctx context.Context, event kafka.PaymentCompletedEvent) error {
		if event.Status != kafka.PaymentStatusSuccess {
			log.Printf("Payment for order %s was not successful: %s", event.OrderID, event.Status)
			return nil
		}
		return order.CompleteOrder(ctx, event.OrderID)
	})

	http.HandleFunc("/createorder", order.OrderCreate)

	fmt.Println("Server started...")

	if err := http.ListenAndServe(":8082", nil); err != nil {
		log.Println(err)
	}
}
