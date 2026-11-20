package order

import (
	"command/database"
	"command/kafka"
	"command/models"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

type createOrderRequest struct {
	models.OrderDetails
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

var producer *kafka.Producer

func SetProducer(p *kafka.Producer) {
	producer = p
}

func OrderCreate(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	order := req.OrderDetails
	if order.OrderStatus == "" {
		order.OrderStatus = models.StatusPending
	}

	err = database.Db.WithContext(r.Context()).Create(&order).Error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if producer != nil {
		event := kafka.OrderCreatedEvent{
			EventID:          fmt.Sprintf("order-%d", order.OrderId),
			EventType:        "order.created",
			OrderID:          strconv.Itoa(order.OrderId),
			UserID:           order.UserId,
			ProductID:        order.ProductId,
			Amount:           req.Amount,
			Currency:         req.Currency,
			OrderStatus:      order.OrderStatus,
			OrderCreatedDate: order.OrderCreatedDate,
		}
		if err := producer.PublishOrder(r.Context(), event); err != nil {
			log.Printf("Failed to publish order created event: %v", err)
			http.Error(w, "order created but event could not be published", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int{"order_id": order.OrderId})
}
