package EventsToGo

import (
	"github.com/JKolios/EventsToGo/consumers"
	"github.com/JKolios/EventsToGo/events"
	"github.com/JKolios/EventsToGo/producers"
	"log"
	"strconv"
	"testing"
	"time"
)

const TEST_EVENT_COUNT = 5

var testOutput []events.Event = []events.Event{}

var referenceOutput []string = []string{}

func PopulateReferenceData() {
	for i := 1; i <= TEST_EVENT_COUNT; i++ {
		referenceOutput = append(referenceOutput, strconv.Itoa(i))
	}
}

func ConsumerRunFuction(consumer *consumers.GenericConsumer, event events.Event) {
	log.Println("Running ConsumerRunFuction")
	testOutput = append(testOutput, event)

}

func ProducerRunFuction(producer *producers.GenericActiveProducer) events.Event {
	if producer.RuntimeObjects["numRuns"].(int) == TEST_EVENT_COUNT {
		haltChan := make(chan interface{})
		<-haltChan

	}
	{
		log.Println("Running ProducerRunFuction")
		producer.RuntimeObjects["numRuns"] = producer.RuntimeObjects["numRuns"].(int) + 1

		return events.Event{strconv.Itoa(producer.RuntimeObjects["numRuns"].(int)),
			producer.Name, time.Now(), events.PRIORITY_LOW}

	}

}

func ProducerWaitFunction(producer *producers.GenericActiveProducer) {
	log.Println("Running ProducerWaitFunction")
}

func TestQueueFunctionality(t *testing.T) {

	PopulateReferenceData()

	queue := NewQueue()

	producer := producers.NewGenericActiveProducer("testProd")
	producer.RegisterFunctions(ProducerRunFuction, ProducerWaitFunction, nil)
	producer.RuntimeObjects["numRuns"] = 0
	consumer := consumers.NewGenericConsumer("testCons")
	consumer.RegisterFunctions(ConsumerRunFuction, nil)

	queue.AddConsumer(consumer)
	queue.AddProducer(producer)
	queue.Start()

	time.Sleep(1 * time.Second)

	queue.Stop()

	if len(testOutput) != TEST_EVENT_COUNT {
		t.Error("Wrong number of events returned")
	}

	for i, output := range testOutput {
		if output.Payload.(string) != referenceOutput[i] {
			t.Errorf("Wrong payload for event: %v\n", i)
		}

		if output.Type != "testProd" {
			t.Errorf("Wrong type for event: %v\n", i)
		}
	}

}
