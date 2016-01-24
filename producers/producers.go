package producers

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
	"time"
)

type Producer interface {
	Start(chan<- events.Event)
	Stop()
}

type GenericActiveProducer struct {
	name           string
	outputChan     chan<- events.Event
	waitChan, done chan struct{}
}

func NewProducer(name string, config map[string]string, outputChan chan events.Event) *GenericActiveProducer {

	producer := &GenericActiveProducer{name, outputChan, make(chan struct{}), make(chan struct{})}

	producer.setupFunction(config)
	return producer
}

func (producer *GenericActiveProducer) Start(outputChan chan<- events.Event) {

	go producerCoroutine(producer)
	go timingCoroutine(producer.done, producer.waitChan, producer.waitFunction)
	log.Printf("%v Producer: started\n", producer.name)
}

func (consumer *GenericActiveProducer) Stop() {

	close(consumer.done)
}

func (consumer *GenericActiveProducer) setupFunction(config map[string]string) {}

func (consumer *GenericActiveProducer) runFunction() (events.Event, events.Priority) {
	return events.Event{}, events.PRIORITY_LOW
}

func (consumer *GenericActiveProducer) waitFunction() {}

func (consumer *GenericActiveProducer) stopFunction() {}

func producerCoroutine(producer *GenericActiveProducer) {

	for {
		select {
		case <-producer.done:
			{
				producer.stopFunction()
				log.Printf("%v Producer Terminated\n", producer.name)
				return
			}
		case <-producer.waitChan:
			{
				log.Printf("Starting %v polling\n", producer.name)
				funcResult, priority := producer.runFunction()
				finalEvent := events.Event{funcResult, producer.name, time.Now(), priority}
				producer.outputChan <- finalEvent
				log.Printf("%v polling done\n", producer.name)
			}
		}
	}
}

func timingCoroutine(done chan struct{}, waitChan chan struct{}, waitFunc func()) {
	for {
		select {
		case <-done:
			return
		default:
			waitFunc()
			waitChan <- struct{}{}

		}
	}
}
