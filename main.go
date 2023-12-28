package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	brokerAddress = "localhost:9092"
	// number of parallel producers to be run
	NUM_PRODUCERS = 5
	GroupID       = "json-group"
	// number of parallel consumers to be run
	NUM_CONSUMERS = 5
)

var data []map[string]interface{}

func main() {
	topicName := "json-data"

	file, err := os.Open("datasets/data.json")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	// read our opened file as a byte array.
	byteValue, _ := io.ReadAll(file)
	// unmarshal the byte array into a list of maps
	err = json.Unmarshal(byteValue, &data)
	fmt.Println(data[1])
	if err != nil {
		log.Fatalf(err.Error())
	}

	// defining producer config
	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
	}

	// initializing the producer
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		fmt.Printf("Error creating Kafka producer: %v\n", err)
		return
	}

	// configure the producer to produce data asynchronously
	producer.ProduceChannel()

	// defining consumer config
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"group.id":          GroupID,
		"auto.offset.reset": "earliest",
	}

	// Initializing the consumer
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		fmt.Printf("Error creating Kafka consumer: %v\n", err)
		return
	}

	// Subscribe to topics
	err = consumer.SubscribeTopics([]string{topicName}, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topics: %v\n", err)
		consumer.Close()
		return
	}

	// Handle signals to gracefully shut down the producer and the consumer
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle signals
	go func() {
		<-signals
		fmt.Println("Received interrupt signal. Closing producer...")
		// Flush the producer before closing
		// waiting 15 seconds
		producer.Flush(15 * 1000)
		producer.Close()
		fmt.Println("Received interrupt signal. Closing consumer...")
		consumer.Close()
	}()

	// wait group
	var wg sync.WaitGroup

	// Spawn parallel goroutines = NUM_PRODUCERS
	for i := 0; i < NUM_PRODUCERS; i++ {
		wg.Add(1)
		go sendMessages(producer, &wg, topicName)
	}

	// Spawn parallel goroutines = NUM_CONSUMERS
	for i := 0; i < NUM_CONSUMERS; i++ {
		wg.Add(1)
		go consumeMessages(consumer, &wg)
	}

	// Block execution of main function until all the goroutines are complete
	wg.Wait()

}

func sendMessages(producer *kafka.Producer, wg *sync.WaitGroup,
	topicName string) {

	defer wg.Done()

	// Produce messages infinitely
	for {
		// Choose a random value from data
		randomIndex := rand.Intn(len(data))
		randomValue := data[randomIndex]

		// Marshal the data of type map[string]interface to JSON bytes
		jsonData, err := json.Marshal(randomValue)
		if err != nil {
			log.Printf("JSON Marshall error : %v", err)
			continue
		}

		// construct kafka message
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicName,
				Partition: kafka.PartitionAny,
			},
			Value: jsonData,
		}

		// Produce the message asynchronously
		err = producer.Produce(message, nil)
		if err != nil {
			fmt.Printf("Error producing message : %v\n", err)
		}

		// Introduce a short delay for throttling
		time.Sleep(1 * time.Millisecond)
	}
}

func consumeMessages(consumer *kafka.Consumer, wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		var data_out map[string]interface{}
		// Poll for messages
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			// Processing logic
			// Unmarshal JSON bytes to map[string]interface{}
			err = json.Unmarshal(msg.Value, &data_out)
			// Printing the unmarshalled data
			fmt.Printf("\n%v\n", data_out)
			if err != nil {
				log.Fatalf(err.Error())
			}
		} else {
			// Handle errors
			fmt.Printf("Error reading message: %v\n", err)
		}
	}
}
