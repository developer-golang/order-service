package main

import (
	"command/database"
	"command/migration"
	"command/order"
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
	http.HandleFunc("/createorder", order.OrderCreate)

	fmt.Println("Server started...")

	http.ListenAndServe(":8082", nil)
}
