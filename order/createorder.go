package order

import (
	"command/database"
	"command/models"
	"context"
	"fmt"
	"strconv"
)

func CompleteOrder(ctx context.Context, orderID string) error {
	id, err := strconv.Atoi(orderID)
	if err != nil {
		return fmt.Errorf("invalid order id %q: %w", orderID, err)
	}

	result := database.Db.WithContext(ctx).
		Model(&models.OrderDetails{}).
		Where("order_id = ? AND order_status = ?", id, models.StatusPending).
		Update("order_status", models.StatusCompleted)
	if result.Error != nil {
		return result.Error
	}

	return nil
}
