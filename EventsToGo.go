package EventsToGo

import (
	"log"
	"time"

	"github.com/JKolios/EventsToGo/consumers"
	"github.com/JKolios/EventsToGo/events"
	"github.com/JKolios/EventsToGo/producers"
	"github.com/oleiade/lane"
)

type TaskQueue struct {
	events                             *lane.PQueue
	eventTTL                           *time.Duration
	producers                          []producers.Producer
	producerChan                       chan events.Event
	consumers                          []consumers.Consumer
	consumerChannels                   []chan events.Event
	done                               chan struct{}
	producersRunning, consumersRunning bool
}


func NewQueue(EventTTL *time.Duration) *TaskQueue {

	queue := &TaskQueue{eventTTL: EventTTL}
	queue.events = lane.NewPQueue(lane.MAXPQ)
	queue.done = make(chan struct{})

	return queue
}

func (queue *TaskQueue) AddConsumer(consumer consumers.Consumer) {
	queue.consumers = append(queue.consumers, consumer)

}

func (queue *TaskQueue) AddProducer(producer producers.Producer) {
	queue.producers = append(queue.producers, producer)

}

func (queue *TaskQueue) TaskCount() int {
	return queue.events.Size()

}

func (queue *TaskQueue) Start() {

	if !queue.consumersRunning {
		queue.StartConsumers()
	}

	if !queue.producersRunning {
		queue.StartProducers()
	}

}

func (queue *TaskQueue) StartProducers() {
	if queue.producersRunning {
		log.Println("Producers are already running.")
		return
	}
	queue.producerChan = make(chan events.Event)

	for _, producer := range queue.producers {
		producer.Start(queue.producerChan)
	}

	go queue.producerHub()
	queue.producersRunning = true
}

func (queue *TaskQueue) StartConsumers() {
	if queue.consumersRunning {
		log.Println("Consumers are already running.")
		return
	}
	for _, consumer := range queue.consumers {
		queue.consumerChannels = append(queue.consumerChannels, consumer.Start())
	}

	go queue.consumerHub()
	queue.consumersRunning = true
}

func (queue *TaskQueue) Stop() {

	queue.StopProducers()
	queue.StopConsumers()
	close(queue.done)
}

func (queue *TaskQueue) StopProducers() {
	for _, producer := range queue.producers {
		producer.Stop()
	}
	queue.producersRunning = false
}

func (queue *TaskQueue) StopConsumers() {
	for _, consumer := range queue.consumers {
		consumer.Stop()
	}
	queue.consumersRunning = false
}
