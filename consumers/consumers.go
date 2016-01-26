package consumers

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
)

type Consumer interface {
	Start() chan events.Event
	Stop()
}

type GenericConsumer struct {
	Name           string
	RuntimeObjects map[string]interface{}
	inputChan      chan events.Event
	done           chan struct{}
	runFunction    func(*GenericConsumer, events.Event)
	stopFunction   func(*GenericConsumer)
}

func NewGenericConsumer(name string) *GenericConsumer {
	consumer := &GenericConsumer{Name: name}
	consumer.RuntimeObjects = make(map[string]interface{})
	consumer.inputChan = make(chan events.Event)
	consumer.done = make(chan struct{})

	return consumer
}

func (consumer *GenericConsumer) Start() chan events.Event {

	go consumerCoroutine(consumer)
	log.Printf("%v Consumer: started\n", consumer.Name)
	return consumer.inputChan
}

func (consumer *GenericConsumer) Stop() {

	close(consumer.done)
}

func (consumer *GenericConsumer) RegisterFunctions(runFunction func(*GenericConsumer, events.Event), stopFunction func(*GenericConsumer)) {

	consumer.runFunction = runFunction
	if consumer.runFunction == nil {
		consumer.runFunction = func(c *GenericConsumer, e events.Event) {}
	}

	consumer.stopFunction = stopFunction
	if consumer.stopFunction == nil {
		consumer.stopFunction = func(c *GenericConsumer) {}
	}
}

func consumerCoroutine(consumer *GenericConsumer) {
	for {
		select {
		case <-consumer.done:
			{
				consumer.stopFunction(consumer)
				log.Printf("%v Consumer Terminated\n", consumer.Name)
				return
			}
		case incomingEvent := <-consumer.inputChan:
			consumer.runFunction(consumer, incomingEvent)

		}
	}
}
