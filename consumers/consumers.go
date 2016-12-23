package consumers

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
)

type Consumer interface {
	Start() chan events.Event
	Stop()
	Consume(events.Event)
}

type PlugableMethodConsumer struct {
	Name           string
	config         map[string]interface{}
	RuntimeObjects map[string]interface{}
	inputChan      chan events.Event
	done           chan struct{}
	setupFunction  func(*PlugableMethodConsumer, map[string]interface{})
	runFunction    func(*PlugableMethodConsumer, events.Event)
	stopFunction   func(*PlugableMethodConsumer)
}

func NewPlugableMethodConsumer(name string, config map[string]interface{}) *PlugableMethodConsumer {
	consumer := &PlugableMethodConsumer{Name: name}
	consumer.config = config
	consumer.RuntimeObjects = make(map[string]interface{})
	consumer.inputChan = make(chan events.Event)
	consumer.done = make(chan struct{})

	return consumer
}

func (consumer *PlugableMethodConsumer) Start() chan events.Event {
	consumer.setupFunction(consumer, consumer.config)
	go consumerCoroutine(consumer)
	log.Printf("%v Consumer: started\n", consumer.Name)
	return consumer.inputChan
}

func (consumer *PlugableMethodConsumer) Stop() {

	close(consumer.done)
}

func (consumer *PlugableMethodConsumer) RegisterFunctions(setupFunction func(*PlugableMethodConsumer, map[string]interface{}),
	runFunction func(*PlugableMethodConsumer, events.Event),
	stopFunction func(*PlugableMethodConsumer)) {

	consumer.setupFunction = setupFunction
	if consumer.setupFunction == nil {
		consumer.setupFunction = func(c *PlugableMethodConsumer, s map[string]interface{}) {}
	}

	consumer.runFunction = runFunction
	if consumer.runFunction == nil {
		consumer.runFunction = func(c *PlugableMethodConsumer, e events.Event) {}
	}

	consumer.stopFunction = stopFunction
	if consumer.stopFunction == nil {
		consumer.stopFunction = func(c *PlugableMethodConsumer) {}
	}
}

func (consumer *PlugableMethodConsumer) Consume(incomingEvent events.Event ) {
	consumer.runFunction(consumer, incomingEvent)
}

func consumerCoroutine(consumer *PlugableMethodConsumer) {
	for {
		select {
		case <-consumer.done:
			{
				consumer.stopFunction(consumer)
				log.Printf("%v Consumer Terminated\n", consumer.Name)
				return
			}
		case incomingEvent := <-consumer.inputChan:
			consumer.Consume(incomingEvent)

		}
	}
}
