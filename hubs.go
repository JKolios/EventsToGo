package EventsToGo

import (
	"github.com/JKolios/EventsToGo/events"
	"log"
	"time"
)

func (queue *TaskQueue) producerHub() {

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

func (queue *TaskQueue) consumerHub() {

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