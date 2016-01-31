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

var testOutput []string = []string{}

var referenceOutput []string = []string{}

func PopulateReferenceData() {
	for i := 1; i <= TEST_EVENT_COUNT; i++ {
		referenceOutput = append(referenceOutput, "Producer"+strconv.Itoa(i)+"Consumer")
	}
}

func ConsumerSetupFuction(consumer *consumers.GenericConsumer, config map[string]interface{}) {
	consumer.RuntimeObjects["consumerString"] = config["consumerString"].(string)

}

func ConsumerRunFuction(consumer *consumers.GenericConsumer, event events.Event) {
	testOutput = append(testOutput, event.Payload.(string)+consumer.RuntimeObjects["consumerString"].(string))

}

func ProducerSetupFuction(producer *producers.GenericProducer, config map[string]interface{}) {
	producer.RuntimeObjects["producerString"] = config["producerString"].(string)

}

func ProducerRunFuction(producer *producers.GenericProducer) events.Event {
	if producer.RuntimeObjects["numRuns"].(int) == TEST_EVENT_COUNT {
		haltChan := make(chan interface{})
		<-haltChan

	}
	{
		log.Println("Running ProducerRunFuction")
		producer.RuntimeObjects["numRuns"] = producer.RuntimeObjects["numRuns"].(int) + 1

		return events.Event{producer.RuntimeObjects["producerString"].(string) + strconv.Itoa(producer.RuntimeObjects["numRuns"].(int)),
			producer.Name, time.Now(), events.PRIORITY_LOW}

	}

}

func ProducerWaitFunction(producer *producers.GenericProducer) {
}

func TestQueueFunctionality(t *testing.T) {

	PopulateReferenceData()

	testConfig := map[string]interface{}{"producerString": "Producer", "consumerString": "Consumer"}

	queue := NewQueue(nil)

	producer := producers.NewGenericProducer("testProd", testConfig)
	producer.RegisterFunctions(ProducerSetupFuction, ProducerRunFuction, ProducerWaitFunction, nil)
	producer.RuntimeObjects["numRuns"] = 0
	consumer := consumers.NewGenericConsumer("testCons", testConfig)
	consumer.RegisterFunctions(ConsumerSetupFuction, ConsumerRunFuction, nil)

	queue.AddConsumer(consumer)
	queue.AddProducer(producer)
	queue.Start()

	time.Sleep(1 * time.Second)

	queue.Stop()

	if len(testOutput) != TEST_EVENT_COUNT {
		t.Error("Wrong number of events returned")
	}

	for i, output := range testOutput {
		if output != referenceOutput[i] {
			t.Errorf("Wrong payload for event: %v\n", i)
		}

	}

}
