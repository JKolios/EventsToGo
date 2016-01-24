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
	name      string
	inputChan chan events.Event
	done      chan struct{}
}

func NewConsumer(constructorMap map[string]func() GenericConsumer, name string, config map[string]string) *GenericConsumer {

	consumer := constructorMap[name]()
	consumer.setupFunction(config)
	return &consumer
}

func (consumer *GenericConsumer) Start() chan events.Event {

	go consumerCoroutine(consumer)
	log.Printf("%v Consumer: started\n", consumer.name)
	return consumer.inputChan
}

func (consumer *GenericConsumer) Stop() {

	close(consumer.done)
}

func (consumer *GenericConsumer) setupFunction(config map[string]string) {}

func (consumer *GenericConsumer) runFunction(events.Event) {}

func (consumer *GenericConsumer) stopFunction() {}

func consumerCoroutine(consumer *GenericConsumer) {
	for {
		select {
		case <-consumer.done:
			{
				consumer.stopFunction()
				log.Printf("%v Consumer Terminated\n", consumer.name)
				return
			}
		case incomingEvent := <-consumer.inputChan:
			consumer.runFunction(incomingEvent)

		}
	}
}
