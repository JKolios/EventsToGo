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
	config         map[string]interface{}
	RuntimeObjects map[string]interface{}
	inputChan      chan events.Event
	Done           chan struct{}
	setupFunction  func(*GenericConsumer, map[string]interface{})
	runFunction    func(*GenericConsumer, events.Event)
	stopFunction   func(*GenericConsumer)
}

func NewGenericConsumer(name string, config map[string]interface{}) *GenericConsumer {
	consumer := &GenericConsumer{Name: name}
	consumer.config = config
	consumer.RuntimeObjects = make(map[string]interface{})
	consumer.inputChan = make(chan events.Event)
	consumer.Done = make(chan struct{})

	return consumer
}

func (consumer *GenericConsumer) Start() chan events.Event {
	consumer.setupFunction(consumer, consumer.config)
	go consumerCoroutine(consumer)
	log.Printf("%v Consumer: started\n", consumer.Name)
	return consumer.inputChan
}

func (consumer *GenericConsumer) Stop() {

	close(consumer.Done)
}

func (consumer *GenericConsumer) RegisterFunctions(setupFunction func(*GenericConsumer, map[string]interface{}),
	runFunction func(*GenericConsumer, events.Event),
	stopFunction func(*GenericConsumer)) {

	consumer.setupFunction = setupFunction
	if consumer.setupFunction == nil {
		consumer.setupFunction = func(c *GenericConsumer, s map[string]interface{}) {}
	}

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
		case <-consumer.Done:
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
