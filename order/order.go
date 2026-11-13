package order

import (
	"command/database"
	"command/models"
	"encoding/json"
	"fmt"
	"net/http"
)

func OrderCreate(w http.ResponseWriter, r *http.Request) {
	var order models.OrderDetails
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = database.Db.Create(&order).Error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Order created successfully with ID: %d", order.OrderId)
}
