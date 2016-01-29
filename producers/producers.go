package producers

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
)

type Producer interface {
	Start(chan<- events.Event)
	Stop()
}

type GenericProducer struct {
	Name           string
	config         map[string]interface{}
	RuntimeObjects map[string]interface{}
	outputChan     chan<- events.Event
	waitChan, Done chan struct{}
	setupFunction  func(*GenericProducer, map[string]interface{})
	runFunction    func(*GenericProducer) events.Event
	waitFunction   func(*GenericProducer)
	stopFunction   func(*GenericProducer)
}

func NewGenericProducer(name string, config map[string]interface{}) *GenericProducer {
	producer := &GenericProducer{Name: name}
	producer.config = config
	producer.RuntimeObjects = make(map[string]interface{})
	producer.waitChan = make(chan struct{})
	producer.Done = make(chan struct{})

	return producer
}

func (producer *GenericProducer) RegisterFunctions(setupFunction func(*GenericProducer, map[string]interface{}),
	runFunction func(*GenericProducer) events.Event,
	waitFunction func(*GenericProducer),
	stopFunction func(*GenericProducer)) {

	producer.setupFunction = setupFunction
	if producer.setupFunction == nil {
		producer.setupFunction = func(c *GenericProducer, s map[string]interface{}) {}
	}

	producer.runFunction = runFunction
	if producer.runFunction == nil {
		producer.runFunction = func(p *GenericProducer) events.Event { return events.Event{} }
	}

	producer.waitFunction = waitFunction
	if producer.waitFunction == nil {
		producer.waitFunction = func(p *GenericProducer) {}
	}

	producer.stopFunction = stopFunction
	if producer.stopFunction == nil {
		producer.stopFunction = func(p *GenericProducer) {}
	}
}

func (producer *GenericProducer) Start(outputChan chan<- events.Event) {

	producer.outputChan = outputChan

	producer.setupFunction(producer, producer.config)
	go producerCoroutine(producer)
	go timingCoroutine(producer)
	log.Printf("%v Producer: started\n", producer.Name)
}

func (producer *GenericProducer) Stop() {

	close(producer.Done)
}

func producerCoroutine(producer *GenericProducer) {

	for {
		select {
		case <-producer.Done:
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

func timingCoroutine(producer *GenericProducer) {
	for {
		select {
		case <-producer.Done:
			return
		default:
			producer.waitFunction(producer)
			producer.waitChan <- struct{}{}

		}
	}
}
