package EventsToGo

import (
	"github.com/JKolios/EventsToGo/consumers"
	"github.com/JKolios/EventsToGo/producers"
	"github.com/JKolios/EventsToGo/events"
)

var testData []string = []string{"one", "two", "three"}
var testData []events.Event = []events.Event{"one", "two", "three", "four"}

type TestConsumer struct {
	consumers.GenericConsumer
}

func (consumer *TestConsumer) runFunction(events.Event) {

}

func (consumer TestConsumer)

type TestProducer struct {
	producers.GenericActiveProducer
}


