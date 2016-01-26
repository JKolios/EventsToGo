package producers

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
)

type Producer interface {
	Start(chan<- events.Event)
	Stop()
}

type GenericActiveProducer struct {
	Name           string
	RuntimeObjects map[string]interface{}
	outputChan     chan<- events.Event
	waitChan, done chan struct{}
	runFunction    func(*GenericActiveProducer) events.Event
	waitFunction   func(*GenericActiveProducer)
	stopFunction   func(*GenericActiveProducer)
}

func NewGenericActiveProducer(name string) *GenericActiveProducer {
	producer := &GenericActiveProducer{Name: name}
	producer.RuntimeObjects = make(map[string]interface{})
	producer.waitChan = make(chan struct{})
	producer.done = make(chan struct{})

	return producer
}

func (producer *GenericActiveProducer) RegisterFunctions(runFunction func(*GenericActiveProducer) events.Event, waitFunction func(*GenericActiveProducer), stopFunction func(*GenericActiveProducer)) {

	producer.runFunction = runFunction
	if producer.runFunction == nil {
		producer.runFunction = func(p *GenericActiveProducer) events.Event { return events.Event{} }
	}

	producer.waitFunction = waitFunction
	if producer.waitFunction == nil {
		producer.waitFunction = func(p *GenericActiveProducer) {}
	}

	producer.stopFunction = stopFunction
	if producer.stopFunction == nil {
		producer.stopFunction = func(p *GenericActiveProducer) {}
	}
}

func (producer *GenericActiveProducer) Start(outputChan chan<- events.Event) {
	if producer.runFunction == nil || producer.stopFunction == nil || producer.waitFunction == nil {
		log.Fatalf("Producer %s run without registered functions\n")
	}
	producer.outputChan = outputChan

	go producerCoroutine(producer)
	go timingCoroutine(producer)
	log.Printf("%v Producer: started\n", producer.Name)
}

func (producer *GenericActiveProducer) Stop() {

	close(producer.done)
}

func producerCoroutine(producer *GenericActiveProducer) {

	for {
		select {
		case <-producer.done:
			{
				producer.stopFunction(producer)
				log.Printf("%v Producer Terminated\n", producer.Name)
				return
			}
		case <-producer.waitChan:
			{
				log.Printf("Starting %v polling\n", producer.Name)
				producedEvent := producer.runFunction(producer)
				producer.outputChan <- producedEvent
				log.Printf("%v polling done\n", producer.Name)
			}
		}
	}
}

func timingCoroutine(producer *GenericActiveProducer) {
	for {
		select {
		case <-producer.done:
			return
		default:
			producer.waitFunction(producer)
			producer.waitChan <- struct{}{}

		}
	}
}
