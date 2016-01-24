package inspector

import (
	"container/list"
	"github.com/JKolios/EventsToGo/events"
	"log"
	"time"
)

func ListReport(listToInspect *list.List, listName string, tickPeriod time.Duration) {

	tick := time.Tick(tickPeriod)
	for {
		<-tick
		log.Printf("List Report: %v\n", listName)
		log.Printf("List Length: %v\n", listToInspect.Len())
		i := 0
		var next *list.Element
		for element := listToInspect.Front(); element != nil; element = next {
			// keep a pointer to the next element stored
			// this will be used to continue iteration after calling Remove()
			next = element.Next()
			log.Printf("Element %v: %+v\n", i, element.Value)
			i++
		}

	}

}

func ListCleaner(listToClean *list.List, listName string, tickPeriod time.Duration) {

	tick := time.Tick(tickPeriod)
	for {
		<-tick
		log.Println("Starting cleanup for list:" + listName)
		var next *list.Element
		for element := listToClean.Front(); element != nil; element = next {
			// keep a pointer to the next element stored
			// this will be used to continue iteration after calling Remove()
			next = element.Next()
			if (time.Now().Sub(element.Value.(events.Event).CreatedOn)) > events.MAX_EVENT_LIFETIME {
				log.Printf("Element %+v exceeded TTL, removing\n", element.Value)

				listToClean.Remove(element)
			}

		}

	}

}
