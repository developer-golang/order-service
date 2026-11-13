package models

import "time"

type OrderDetails struct {
	OrderId          int       `json:"order_id" gorm:"type:int;not null;column:order_id;primaryKey;autoIncrement"`
	UserId           int       `json:"user_id" gorm:"type:int;not null;column:user_id"`
	ProductId        int       `json:"product_id" gorm:"type:int;not null;column:product_id"`
	OrderStatus      string    `json:"order_status" gorm:"type:varchar(50);not null;column:order_status"`
	OrderCreatedDate time.Time `json:"order_created_date" gorm:"type:timestamp;not null;column:order_created_date"`
}
