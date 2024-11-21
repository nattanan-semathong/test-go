package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
	"golang.org/x/exp/rand"
)

var redisClient *redis.Client
var kafkaWriter *kafka.Writer
var kafkaNotiWriter *kafka.Writer
var ctx = context.Background()

type MenuItem struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Price       float64 `json:"price"`
	Description string  `json:"description"`
}

type RestaurantMenu struct {
	RestaurantID string     `json:"restaurant_id"`
	Menu         []MenuItem `json:"menu"`
}

type Restaurant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Rider struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type OrderItem struct {
	MenuID   string `json:"menu_id"`
	Quantity int    `json:"quantity"`
}

type Order struct {
	OrderID      string      `json:"order_id"`
	RestaurantID string      `json:"restaurant_id"`
	Items        []OrderItem `json:"items"`
	TotalAmount  float64     `json:"total_amount"`
	Status       string      `json:"status"`
}

type AcceptOrderRequest struct {
	OrderID      string `json:"order_id"`
	RestaurantID string `json:"restaurant_id"`
}

type AcceptOrderResponse struct {
	Status string `json:"status"`
}

type PickupRequest struct {
	OrderID string `json:"order_id"`
	RiderID string `json:"rider_id"`
}

type DeliverRequest struct {
	OrderID string `json:"order_id"`
	RiderID string `json:"rider_id"`
}

type SendNotificationRequest struct {
	Recipient string `json:"recipient"`
	OrderID   string `json:"order_id"`
	Message   string `json:"message"`
}

func main() {
	e := echo.New()

	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	}

	kafkaNotiWriter =
		&kafka.Writer{
			Addr:     kafka.TCP("localhost:9092"),
			Topic:    "order-delivered",
			Balancer: &kafka.LeastBytes{},
		}

	e.GET("/menu", getMenu)
	e.GET("/restaurant", getRestaurant)
	e.GET("/rider", getRider)
	e.POST("/order", placeOrder)
	e.POST("/restaurant/order/accept", acceptOrder)
	e.POST("/rider/order/pickup", confirmPickup)
	e.POST("/rider/order/deliver", confirmDelivery)
	e.POST("/notification/send", sendNotification)

	go consumeOrderDeliveredEvent()

	e.Logger.Fatal(e.Start(":8080"))
}

func getMenu(c echo.Context) error {
	restaurantID := c.QueryParam("restaurant_id")
	if restaurantID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "restaurant_id is required"})
	}

	fmt.Printf("view menu called")

	menuData, err := redisClient.Get(ctx, restaurantID).Result()
	if err == redis.Nil {
		fmt.Println("Cache miss, fetching from database...")
		menu, err := fetchMenuFromJSON(restaurantID)
		if err != nil {
			fmt.Printf("Error fetching menu from database: %v\n", err)
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch menu"})
		}

		menuJSON, _ := json.Marshal(menu)
		redisClient.Set(ctx, restaurantID, menuJSON, time.Hour)

		fmt.Printf("view menu from file")
		return c.JSON(http.StatusOK, menu)
	} else if err != nil {
		fmt.Printf("Error fetching from Redis: %v\n", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Redis error"})
	}

	fmt.Printf("view menu from cached")
	var cachedMenu RestaurantMenu
	err = json.Unmarshal([]byte(menuData), &cachedMenu)
	if err != nil {
		fmt.Printf("Error unmarshaling cached menu: %v\n", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to parse cached menu"})
	}
	return c.JSON(http.StatusOK, cachedMenu)
}

func fetchMenuFromJSON(restaurantID string) (RestaurantMenu, error) {
	filePath := "menu.json"
	file, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filePath, err)
		return RestaurantMenu{}, err
	}

	fmt.Println("File contents:", string(file))

	var menuData struct {
		RestaurantID string     `json:"restaurant_id"`
		Menu         []MenuItem `json:"menu"`
	}
	err = json.Unmarshal(file, &menuData)
	if err != nil {
		fmt.Printf("Error unmarshaling JSON: %v\n", err)
		return RestaurantMenu{}, err
	}

	fmt.Printf("Parsed menu data: %+v\n", menuData)

	if menuData.RestaurantID != restaurantID {
		fmt.Printf("Restaurant ID mismatch: expected %s, got %s\n", restaurantID, menuData.RestaurantID)
		return RestaurantMenu{}, fmt.Errorf("menu for restaurant %s not found", restaurantID)
	}

	return RestaurantMenu{
		RestaurantID: menuData.RestaurantID,
		Menu:         menuData.Menu,
	}, nil
}

func getRestaurant(c echo.Context) error {
	fmt.Println("view restaurant called")
	restaurantData, err := redisClient.Get(ctx, "restaurant").Result()
	if err == redis.Nil {
		restaurant, err := fetchRestaurantFromJSON("restaurants.json")
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch restaurant"})
		}

		restaurantJSON, _ := json.Marshal(restaurant)
		redisClient.Set(ctx, "restaurant", restaurantJSON, time.Hour)

		fmt.Println("view restaurant from file")
		return c.JSON(http.StatusOK, map[string]interface{}{"restaurant": restaurant})
	} else if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Redis error"})
	}

	var cachedRestaurant []Restaurant
	err = json.Unmarshal([]byte(restaurantData), &cachedRestaurant)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to parse cached restaurant"})
	}
	fmt.Println("view restaurant from cached")

	return c.JSON(http.StatusOK, map[string]interface{}{"restaurant": cachedRestaurant})
}

func fetchRestaurantFromJSON(filePath string) ([]Restaurant, error) {
	fmt.Println("view rider called")

	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	var data struct {
		Restaurant []Restaurant `json:"restaurant"`
	}
	err = json.Unmarshal(file, &data)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON: %w", err)
	}

	return data.Restaurant, nil
}

func getRider(c echo.Context) error {
	fmt.Println("view rider called")
	riderData, err := redisClient.Get(ctx, "rider").Result()
	if err == redis.Nil {
		riders, err := fetchRidersFromJSON("rider.json")
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch rider"})
		}

		riderJSON, _ := json.Marshal(riders)
		redisClient.Set(ctx, "rider", riderJSON, time.Hour)

		fmt.Println("view rider from file")

		return c.JSON(http.StatusOK, map[string]interface{}{"rider": riders})
	} else if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Redis error"})
	}

	fmt.Println("view rider from cached")
	var cachedRiders []Rider
	err = json.Unmarshal([]byte(riderData), &cachedRiders)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to parse cached rider"})
	}
	return c.JSON(http.StatusOK, map[string]interface{}{"rider": cachedRiders})
}

func fetchRidersFromJSON(filePath string) ([]Rider, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	var data struct {
		Rider []Rider `json:"rider"`
	}
	err = json.Unmarshal(file, &data)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON: %w", err)
	}

	return data.Rider, nil
}

func placeOrder(c echo.Context) error {
	var order Order
	if err := c.Bind(&order); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid order details"})
	}

	if order.RestaurantID == "" || order.Items == nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "restaurant_id and items are required"})
	}

	menu, err := getMenuFromCache(order.RestaurantID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch restaurant menu"})
	}

	totalAmount := 0.0
	for _, item := range order.Items {
		for _, menuItem := range menu.Menu {
			if item.MenuID == menuItem.ID {
				totalAmount += menuItem.Price * float64(item.Quantity)
			}
		}
	}

	order.OrderID = fmt.Sprintf("%d", rand.Intn(10000))
	order.TotalAmount = totalAmount

	order.Status = "created"

	log.Printf("Order information: RestaurantID: %s,OrderID: %s, Menu: %+v, Total Amount: %f", order.RestaurantID, order.OrderID, order.Items, order.TotalAmount)
	err = publishOrderEvent(order)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to publish order event"})
	}

	log.Printf("information order id %s has been paid with order total amount", order.OrderID)

	return c.JSON(http.StatusOK, map[string]interface{}{
		"order_id": order.OrderID,
		"status":   order.Status,
	})

}

func getMenuFromCache(restaurantID string) (RestaurantMenu, error) {
	menuData, err := redisClient.Get(ctx, restaurantID).Result()
	if err == redis.Nil {
		return fetchMenuFromFile(restaurantID)
	} else if err != nil {
		return RestaurantMenu{}, fmt.Errorf("redis error: %v", err)
	}

	var menu RestaurantMenu
	err = json.Unmarshal([]byte(menuData), &menu)
	if err != nil {
		return RestaurantMenu{}, fmt.Errorf("failed to parse cached menu: %v", err)
	}

	return menu, nil
}

func fetchMenuFromFile(restaurantID string) (RestaurantMenu, error) {
	filePath := "menu.json"
	file, err := os.ReadFile(filePath)
	if err != nil {
		return RestaurantMenu{}, fmt.Errorf("failed to read menu file: %v", err)
	}

	var menuData RestaurantMenu
	err = json.Unmarshal(file, &menuData)
	if err != nil {
		return RestaurantMenu{}, fmt.Errorf("failed to parse menu JSON: %v", err)
	}

	if menuData.RestaurantID != restaurantID {
		return RestaurantMenu{}, fmt.Errorf("menu for restaurant %s not found", restaurantID)
	}

	menuJSON, _ := json.Marshal(menuData)
	redisClient.Set(ctx, restaurantID, menuJSON, time.Hour)

	return menuData, nil
}

func publishOrderEvent(order Order) error {
	message := fmt.Sprintf("Order Created: %s | Restaurant: %s | Total: %.2f", order.OrderID, order.RestaurantID, order.TotalAmount)

	err := kafkaWriter.WriteMessages(ctx, kafka.Message{
		Value: []byte(message),
	})
	if err != nil {
		return fmt.Errorf("failed to publish order event to Kafka: %v", err)
	}

	log.Printf("Order event published: %s", message)
	return nil
}

func acceptOrder(c echo.Context) error {
	var req AcceptOrderRequest

	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
	}

	if req.OrderID == "" || req.RestaurantID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing order_id or restaurant_id"})
	}

	fmt.Printf("Accepting order with ID: %s for restaurant ID: %s\n", req.OrderID, req.RestaurantID)

	resp := AcceptOrderResponse{
		Status: "accepted",
	}

	err := publishAcceptOrderEvent(req.OrderID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, resp)
}

func publishAcceptOrderEvent(orderID string) error {
	message := fmt.Sprintf("Order %s Accept Order", orderID)
	log.Printf("Publishing to Kafka: %s", message)

	err := kafkaWriter.WriteMessages(context.TODO(), kafka.Message{
		Value: []byte(message),
	})
	if err != nil {
		return fmt.Errorf("failed to publish to Kafka: %v", err)
	}

	log.Printf("Event published to Kafka: %s", message)
	return nil
}

func confirmPickup(c echo.Context) error {
	var req PickupRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	log.Printf("Rider %s confirmed pickup for order %s", req.RiderID, req.OrderID)

	err := publishConfirmPickupEvent(req.OrderID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "picked_up"})
}

func publishConfirmPickupEvent(orderID string) error {
	message := fmt.Sprintf("Order %s Confirm Pickup", orderID)
	log.Printf("Publishing to Kafka: %s", message)

	err := kafkaWriter.WriteMessages(context.TODO(), kafka.Message{
		Value: []byte(message),
	})
	if err != nil {
		return fmt.Errorf("failed to publish to Kafka: %v", err)
	}

	log.Printf("Event published to Kafka: %s", message)
	return nil
}

func confirmDelivery(c echo.Context) error {
	var req DeliverRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	if req.OrderID == "" || req.RiderID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Missing order_id or rider_id"})
	}

	log.Printf("Rider %s delivering order %s", req.RiderID, req.OrderID)

	err := publishOrderDeliveredEvent(req.OrderID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "Delivered"})
}

func publishOrderDeliveredEvent(orderID string) error {
	message := fmt.Sprintf("Order %s Delivered", orderID)
	log.Printf("Publishing to Kafka: %s", message)

	err := kafkaWriter.WriteMessages(context.TODO(), kafka.Message{
		Value: []byte(message),
	})
	if err != nil {
		return fmt.Errorf("failed to publish to Kafka: %v", err)
	}

	log.Printf("Event published to Kafka: %s", message)
	return nil
}

func sendNotification(c echo.Context) error {
	var req SendNotificationRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	if req.Recipient != "customer" && req.Recipient != "restaurant" && req.Recipient != "rider" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid recipient"})
	}

	log.Printf("Sending notification to %s for order %s: %s", req.Recipient, req.OrderID, req.Message)

	return c.JSON(http.StatusOK, map[string]string{"status": "sent"})
}

func consumeOrderDeliveredEvent() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "notification-service-group",
		Topic:   "orders",
	})

	for {
		msg, err := r.ReadMessage(context.TODO())
		if err != nil {
			log.Fatalf("error reading message: %v", err)
		}

		r.CommitMessages(context.Background(), msg)
		processOrderDeliveredEvent(string(msg.Value))
	}
}

func processOrderDeliveredEvent(message string) {
	kafkaNotiWriter.WriteMessages(context.TODO(), kafka.Message{
		Value: []byte("Notification: " + message),
	})
	log.Printf("Notification: %s", message)
}
