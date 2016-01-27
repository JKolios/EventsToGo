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
	config         map[string]interface{}
	RuntimeObjects map[string]interface{}
	outputChan     chan<- events.Event
	waitChan, done chan struct{}
	setupFunction  func(*GenericActiveProducer, map[string]interface{})
	runFunction    func(*GenericActiveProducer) events.Event
	waitFunction   func(*GenericActiveProducer)
	stopFunction   func(*GenericActiveProducer)
}

func NewGenericActiveProducer(name string, config map[string]interface{}) *GenericActiveProducer {
	producer := &GenericActiveProducer{Name: name}
	producer.config = config
	producer.RuntimeObjects = make(map[string]interface{})
	producer.waitChan = make(chan struct{})
	producer.done = make(chan struct{})

	return producer
}

func (producer *GenericActiveProducer) RegisterFunctions(setupFunction func(*GenericActiveProducer, map[string]interface{}),
	runFunction func(*GenericActiveProducer) events.Event,
	waitFunction func(*GenericActiveProducer),
	stopFunction func(*GenericActiveProducer)) {

	producer.setupFunction = setupFunction
	if producer.setupFunction == nil {
		producer.setupFunction = func(c *GenericActiveProducer, s map[string]interface{}) {}
	}

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

	producer.outputChan = outputChan

	producer.setupFunction(producer, producer.config)
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
