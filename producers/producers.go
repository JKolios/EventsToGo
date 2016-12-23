package producers

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
)

type Producer interface {
	Start(chan<- events.Event)
	Stop()
	Produce()
}

type PlugableMethodProducer struct {
	Name           string
	config         map[string]interface{}
	RuntimeObjects map[string]interface{}
	outputChan     chan<- events.Event
	waitChan, Done chan struct{}
	setupFunction  func(*PlugableMethodProducer, map[string]interface{})
	runFunction    func(*PlugableMethodProducer) events.Event
	waitFunction   func(*PlugableMethodProducer)
	stopFunction   func(*PlugableMethodProducer)
}

func NewPlugableMethodProducer(name string, config map[string]interface{}) *PlugableMethodProducer {
	producer := &PlugableMethodProducer{Name: name}
	producer.config = config
	producer.RuntimeObjects = make(map[string]interface{})
	producer.waitChan = make(chan struct{})
	producer.Done = make(chan struct{})

	return producer
}

func (producer *PlugableMethodProducer) RegisterFunctions(setupFunction func(*PlugableMethodProducer, map[string]interface{}),
	runFunction func(*PlugableMethodProducer) events.Event,
	waitFunction func(*PlugableMethodProducer),
	stopFunction func(*PlugableMethodProducer)) {

	producer.setupFunction = setupFunction
	if producer.setupFunction == nil {
		producer.setupFunction = func(c *PlugableMethodProducer, s map[string]interface{}) {}
	}

	producer.runFunction = runFunction
	if producer.runFunction == nil {
		producer.runFunction = func(p *PlugableMethodProducer) events.Event { return events.Event{} }
	}

	producer.waitFunction = waitFunction
	if producer.waitFunction == nil {
		producer.waitFunction = func(p *PlugableMethodProducer) {}
	}

	producer.stopFunction = stopFunction
	if producer.stopFunction == nil {
		producer.stopFunction = func(p *PlugableMethodProducer) {}
	}
}

func (producer *PlugableMethodProducer) Start(outputChan chan<- events.Event) {

	producer.outputChan = outputChan

	producer.setupFunction(producer, producer.config)
	go producerCoroutine(producer)
	go timingCoroutine(producer)
	log.Printf("%v Producer: started\n", producer.Name)
}

func (producer *PlugableMethodProducer) Stop() {

	close(producer.Done)
}

func (producer * PlugableMethodProducer) Produce() {
	producedEvent := producer.runFunction(producer)
	producer.outputChan <- producedEvent
}


func producerCoroutine(producer *PlugableMethodProducer) {

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
				producer.Produce()
				log.Printf("%v polling done\n", producer.Name)
			}
		}
	}
}

func timingCoroutine(producer *PlugableMethodProducer) {
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
