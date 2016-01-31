package EventsToGo

import (
	"log"
	"time"

	"github.com/JKolios/EventsToGo/consumers"
	"github.com/JKolios/EventsToGo/events"
	"github.com/JKolios/EventsToGo/producers"
	"github.com/oleiade/lane"
)

type EventQueue struct {
	events           *lane.PQueue
	eventTTL         *time.Duration
	producers        []producers.Producer
	producerChan     chan events.Event
	consumers        []consumers.Consumer
	consumerChannels []chan events.Event
	done             chan struct{}
}

func (queue *EventQueue) producerHub() {

	var incomingEvent events.Event

	for {
		select {
		case <-queue.done:
			log.Println("producerHub halting")
			return

		case incomingEvent = <-queue.producerChan:
			log.Printf("producerHub got event: %+v\n", incomingEvent)
			// Inspect the event and handle according to type and priority
			queue.events.Push(incomingEvent, incomingEvent.Priority)

		}
	}
}

func (queue *EventQueue) consumerHub() {

	for {
		select {

		case <-queue.done:
			log.Println("consumerHub halting")
			return

		default:

			tempSelectedEvent, _ := queue.events.Pop()
			if tempSelectedEvent != nil {
				selectedEvent := tempSelectedEvent.(events.Event)
				log.Printf("consumerHub dequeued event: %+v \n", selectedEvent)

				if (queue.eventTTL == nil) || (time.Now().Sub(selectedEvent.CreatedOn) < *queue.eventTTL) {
					for _, consumerChan := range queue.consumerChannels {
						consumerChan <- selectedEvent
					}
					log.Println("event dispatched to consumers")

				} else {
					log.Println("event exceeded TTL, discarding")

				}
			}

		}
	}
}

func NewQueue(EventTTL *time.Duration) *EventQueue {

	queue := &EventQueue{eventTTL: EventTTL}
	queue.events = lane.NewPQueue(lane.MAXPQ)
	queue.done = make(chan struct{})

	return queue
}

func (queue *EventQueue) AddConsumer(consumer consumers.Consumer) {
	queue.consumers = append(queue.consumers, consumer)

}

func (queue *EventQueue) AddProducer(producer producers.Producer) {
	queue.producers = append(queue.producers, producer)

}

func (queue *EventQueue) Start() {

	queue.producerChan = make(chan events.Event)

	for _, producer := range queue.producers {
		producer.Start(queue.producerChan)
	}

	for _, consumer := range queue.consumers {
		queue.consumerChannels = append(queue.consumerChannels, consumer.Start())
	}

	go queue.producerHub()
	go queue.consumerHub()

}

func (queue *EventQueue) Stop() {
	close(queue.done)

	for _, producer := range queue.producers {
		producer.Stop()
	}

	for _, consumer := range queue.consumers {
		consumer.Stop()
	}
}
