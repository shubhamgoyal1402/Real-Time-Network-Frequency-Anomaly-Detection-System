package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	anomalyCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "anomaly_detected_total",
			Help: "Total number of anomalies detected.",
		},
	)
	frequencyGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "current_frequency",
			Help: "Current network frequency being processed.",
		},
	)
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(anomalyCount)
	prometheus.MustRegister(frequencyGauge)
}
func startPrometheusMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Println("Starting Prometheus metrics endpoint at :9091/metrics")
		log.Fatal(http.ListenAndServe(":9091", nil))
	}()
}

// MongoDB Setup
var mongoClient *mongo.Client
var frequencyCollection *mongo.Collection

// MongoDB Connection
func connectToMongoDB() error {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	frequencyCollection = mongoClient.Database("network_data").Collection("frequency")
	return nil
}

// Insert frequency data into MongoDB
func storeFrequencyInDB(frequency float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	document := bson.D{
		{Key: "frequency", Value: frequency},
		{Key: "timestamp", Value: time.Now()},
	}

	_, err := frequencyCollection.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to insert document: %v", err)
	}
	log.Printf("Stored frequency: %.2f in MongoDB\n", frequency)
	return nil
}

// Call Python API for anomaly detection
func callPythonAnomalyAPI(frequency float64) (bool, error) {
	apiURL := "http://localhost:5000/predict"

	payload := map[string]interface{}{
		"frequency": frequency,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("error creating JSON payload: %v", err)
	}

	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return false, fmt.Errorf("error sending request to Python API: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("error decoding Python API response: %v", err)
	}

	return result["anomaly"].(bool), nil
}

// Send Email Alert using SMTP

func consumeMessages(brokers []string, topic string, group string) error {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)

	go func() {
		for {
			if err := consumer.Consume(ctx, []string{topic}, consumerHandler{}); err != nil {
				log.Printf("Error in consumer: %v", err)
			}
			if ctx.Err() != nil {
				break
			}
		}
	}()

	<-sigterm
	cancel()
	log.Println("Terminating consumer...")
	time.Sleep(1 * time.Second)
	return nil
}

// Data Preprocessing Functions

// NormalizeData takes the frequency and normalizes it by rounding to 2 decimal places
func NormalizeData(frequency float64) float64 {
	return float64(int(frequency*100)) / 100
}

// Alerts to mail
func sendEmailAlertSMTP(frequency float64) error {
	// Set up authentication information.
	smtpServer := "smtp.gmail.com"
	smtpPort := "587"
	sender := "shubhamgoyal1402@gmail.com"
	recipient := "nikhil.jagyasi03@gmail.com"
	password := "bbrv jfhv wjbp aicw"

	subject := "Anomaly Detected in Network Frequency"
	body := fmt.Sprintf("ALERT: Anomaly detected! The frequency is %.2f Hz", frequency)
	message := fmt.Sprintf("From: %s\nTo: %s\nSubject: %s\n\n%s", sender, recipient, subject, body)

	auth := smtp.PlainAuth("", sender, password, smtpServer)

	// Send email using SMTP
	err := smtp.SendMail(smtpServer+":"+smtpPort, auth, sender, []string{recipient}, []byte(message))
	if err != nil {
		log.Printf("Error sending email: %v", err)
		return err
	}

	log.Println("Email sent successfully!")
	return nil
}

// Consumer handler for messages
type consumerHandler struct{}

func (consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		frequencyStr := string(msg.Value)

		frequency, err := strconv.ParseFloat(strings.TrimSpace(frequencyStr), 64)
		if err != nil {
			log.Printf("Invalid data format: %s\n", frequencyStr)
			continue
		}

		normalizedFrequency := NormalizeData(frequency)

		err = storeFrequencyInDB(normalizedFrequency)
		if err != nil {
			log.Printf("Error storing frequency in MongoDB: %v", err)
		}

		// Update Prometheus metrics
		frequencyGauge.Set(normalizedFrequency)

		isAnomaly, err := callPythonAnomalyAPI(normalizedFrequency)
		if err != nil {
			log.Printf("Error calling Python API: %v", err)
			continue
		}

		if isAnomaly {
			anomalyCount.Inc() // Increment anomaly count in Prometheus
			log.Printf("ALERT: Anomaly detected for frequency %.2f", normalizedFrequency)
			sendEmailAlertSMTP(normalizedFrequency)

		}

		sess.MarkMessage(msg, "")
	}
	return nil
}
func main() {

	//starte prometheus
	startPrometheusMetrics()
	// MongoDB connection
	err := connectToMongoDB()
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(context.TODO()); err != nil {
			log.Fatalf("Error disconnecting from MongoDB: %v", err)
		}
	}()

	brokers := []string{"localhost:9092"}
	topic := "network_frequency"
	group := "frequency_group"

	err = consumeMessages(brokers, topic, group)
	if err != nil {
		log.Fatalf("Error consuming messages: %v", err)
	}
}
