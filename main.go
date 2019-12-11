package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers                       []string = []string{"localhost:9092"}
	group                                  = "5518-dev"
	topics                                 = "cpc-postitem-sync-5518"
	runtimeContextEnvironmentType          = "dev"
)

const (
	retryInitialIntervalSeconds int = 1
	retryMultiplier             int = 2
	retryMaxIntervalSeconds     int = 120
)

func main() {
	//	initConfiguration()
	config := configConsumer()
	startConsumerGroup(config)
}

func configConsumer() *sarama.Config {
	var config = sarama.NewConfig()

	config.Version = sarama.V0_11_0_2
	//default
	config.Consumer.Return.Errors = false
	//default range
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	//config.Consumer.Offsets.CommitInterval = 10 * time.Second

	//GROUP_ID_CONFIG, a paramter in NewConsumerGroup function
	//BOOTSTRAP_SERVER_CONFIG, a parameter in NewConsumerGroup function

	return config
}

func startConsumerGroup(config *sarama.Config) {
	//context
	ctx, cancel := context.WithCancel(context.Background())
	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	//A WaitGroup waits for a collection of goroutines to finish
	waitGroup := &sync.WaitGroup{}
	//set the number of goroutines wait for. set to concurrency
	waitGroup.Add(1)

	startConsumer(waitGroup, &consumerGroup, &ctx)

	//A Signal represents an operating system signal
	sigterm := make(chan os.Signal, 1)
	// relay incoming signals to channel sigterm
	// SIGINT, user send INTR, triged by Ctrl+C
	// SIGTERM, process end
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}

	//CancelFunc
	cancel()
	//Wait blocks until the WaitGroup counter is zero.
	waitGroup.Wait()
	if err = consumerGroup.Close(); err != nil {
		log.Panicf("Error closing consumer group client: %v", err)
	}

}

func startConsumer(waitGroup *sync.WaitGroup, consumerGroup *sarama.ConsumerGroup, ctx *context.Context) {
	//start go routine
	//chan used to communicate through goroutines
	var consumer = Consumer{
		ready: make(chan bool),
	}

	go func() {
		defer (*waitGroup).Done()
		var retries = 0

		for {

			//Consumer has implement ConsumerGroupHandler interface
			err := (*consumerGroup).Consume(*ctx, strings.Split(topics, ","), &consumer)
			if err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if err = (*ctx).Err(); err != nil {
				//log.Panicf("Error from ctx: %v", err)
				return
			}

			//back off
			retries++
			interval := calculateBackOffTime(retries)
			time.Sleep(interval)
			log.Println("Retry, backoff time", interval)
			consumer.ready = make(chan bool)
		}
	}()

	//block, until the consumer has bean set up
	<-consumer.ready
	log.Println("Sarama consumer up and running!...")
}

func calculateBackOffTime(retries int) time.Duration {
	interval := retryInitialIntervalSeconds
	if retries > 8 {
		interval = retryMaxIntervalSeconds
	} else {
		interval = int(math.Pow(float64(retryMultiplier), float64(retries-1)))
	}
	return time.Duration(interval) * time.Second
}

//Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

//Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	//Mark the consumer as ready
	close(consumer.ready)
	return nil
}

//Cleanup is run at the end of a session, once call ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("consumer clean up")
	return nil
}

//ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	//The ConsumeClaim itself is called within a goroutine
	for message := range claim.Messages() {
		log.Printf("Message claimed: key = %s, value = %s, timestamp = %v, topic = %s, partition = %v, offset = %v",
			string(message.Key), string(message.Value), message.Timestamp,
			message.Topic, message.Partition, message.Offset)

		err := consumeMessage(message)
		if err != nil {
			log.Printf("Message consume fail, error: %s", err)
		} else {
			//Mark message as consumed
			session.MarkMessage(message, "")
		}

	}
	return nil
}

func consumeMessage(message *sarama.ConsumerMessage) error {
	if string((message).Value) == "fail" {
		return errors.New("consume message error, business error")
	}
	return nil
}

//Configuration is used to manage global property
//Capitalized files
type Configuration struct {
	KafkaBootstrapServers []string
	KafkaTopicName        string
	StoreNumber           string
}

func initConfiguration() {
	file, err := os.Open("config.json")
	if err != nil {
		log.Panicln("Reading config.json error:", err)
	}

	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err = decoder.Decode(&configuration)
	if err != nil {
		log.Panicln("Reading configuration error:", err)
	}

	configuration.StoreNumber = os.Getenv("StoreNumber")

	//brokers, group, topics
	brokers = configuration.KafkaBootstrapServers
	group = configuration.StoreNumber + "-" + runtimeContextEnvironmentType
	topics = configuration.KafkaTopicName + configuration.StoreNumber
}
