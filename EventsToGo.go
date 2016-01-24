package EventsToGo

import (
	"container/list"

	"log"
	"time"

	"github.com/JKolios/EventsToGo/consumers"
	"github.com/JKolios/EventsToGo/events"
	"github.com/JKolios/EventsToGo/inspector"
	"github.com/JKolios/EventsToGo/producers"
)

type EventQueue struct {
	highPriorityeventList, lowPriorityeventList list.List
	producers                                   []producers.Producer
	producerChan                                chan events.Event
	consumerChannels                            []chan events.Event
	consumers                                   []consumers.Consumer
	done                                        chan struct{}
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
			if incomingEvent.Priority == events.PRIORITY_HIGH {
				queue.highPriorityeventList.PushBack(incomingEvent)
			} else {
				queue.lowPriorityeventList.PushBack(incomingEvent)
			}

		}
	}
}

func (queue *EventQueue) consumerHub() {

	var selectedEvent events.Event

	for {
		select {

		case <-queue.done:
			log.Println("consumerHub halting")
			return

		default:

			if queue.highPriorityeventList.Len() > 0 {
				selectedEvent = queue.highPriorityeventList.Remove(queue.highPriorityeventList.Front()).(events.Event)
			} else if queue.lowPriorityeventList.Len() > 0 {
				selectedEvent = queue.lowPriorityeventList.Remove(queue.lowPriorityeventList.Front()).(events.Event)
			} else {
				continue
			}

			log.Printf("consumerHub selected event: %+v \n", selectedEvent)

			for _, consumerChan := range queue.consumerChannels {
				consumerChan <- selectedEvent
			}

		}
	}
}

func NewQueue(producerConstructorMap map[string]func(string, map[string]string) producers.Producer,
	consumerConstructorMap map[string]func(string, map[string]string) consumers.Consumer,
	producerConf, consumerConf map[string]map[string]string) *EventQueue {

	queue := &EventQueue{}

	queue.done = make(chan struct{})

	// Consumer Init
	for consumerName, consumerSettings := range consumerConf {
		consumer := consumerConstructorMap[consumerName](consumerName, consumerSettings)
		queue.consumers = append(queue.consumers, consumer)

	}

	// Producer Init
	for producerName, producerSettings := range producerConf {
		producer := producerConstructorMap[producerName](producerName, producerSettings)
		queue.producers = append(queue.producers, producer)

	}

	return queue
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

	go inspector.ListReport(&queue.lowPriorityeventList, "Low Priority", time.Minute*10)
	go inspector.ListReport(&queue.highPriorityeventList, "High Priority", time.Minute*10)

	go inspector.ListCleaner(&queue.lowPriorityeventList, "Low Priority", time.Minute*10)
	go inspector.ListCleaner(&queue.highPriorityeventList, "High Priority", time.Minute*10)
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
