package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	NATS_SERVER_URL string = "192.168.3.34:4222"
	PRODUCER_COUNT  int    = 2
)

type IPublisher interface {
	publishMessages(wg *sync.WaitGroup, id int, nc *nats.Conn, subject string, msgCount int, interval time.Duration)
}

type SimpleTestPublisher struct{}

func (p *SimpleTestPublisher) publishMessages(wg *sync.WaitGroup, id int, nc *nats.Conn, subject string, msgCount int, interval time.Duration) {
	defer wg.Done()
	slog.Info("Starting simple publisher go routine", "id", id, "msgCount", msgCount, "interval", interval)

	for i := 0; i < msgCount; i++ {
		msg := fmt.Sprintf("msg number: %d", i)
		err := nc.Publish(subject, []byte(msg))
		if err != nil {
			slog.Error("error publishing message to NATS", "id", id, "count", i, "error", err)
			continue
		}
		slog.Info("Published message", "id", id, "msg", i, "msgCount", msgCount, "interval", interval)
		time.Sleep(interval)
	}
	slog.Info("Publisher go routine finished", "id", id, "msgCount", msgCount, "interval", interval)
}

type Publisher struct{}

func (p *Publisher) publishMessages(wg *sync.WaitGroup, id int, nc *nats.Conn, subject string, msgCount int, interval time.Duration) {
	defer wg.Done()

	slog.Info("Starting publisher go routine", "id", id, "msgCount", msgCount, "interval", interval)

	data := make([]float64, 100)

	for i := 0; i < msgCount; i++ {
		data = data[:0]
		for j := 0; j < 100; j++ {
			randMeasurement := rand.Float64()*10 - 80 // Generate a random float between -80 and -70
			data = append(data, randMeasurement)
		}

		encodedData, err := encodeFloat64Array(data)
		if err != nil {
			slog.Error("Error encoding data to binary in publisher", "id", id, "subject", subject, "error", err)
			continue
		}

		err = nc.Publish(subject, encodedData)
		if err != nil {
			slog.Error("Error publishing data to NATS", "id", id, "subject", subject, "error", err)
			continue
		}

		// slog.Info("Successully published message", "id", id, "subject", subject)
		time.Sleep(interval)
	}

}

func main() {
	nc, err := nats.Connect(NATS_SERVER_URL)
	if err != nil {
		slog.Error("error connecting to NATS server", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	slog.Info("Connected to NATS server", "serverURL", NATS_SERVER_URL)
	var wg sync.WaitGroup

	// sp := SimpleTestPublisher{}
	// for i := 0; i < PRODUCER_COUNT; i++ {
	// 	wg.Add(1)
	// 	subj := fmt.Sprintf("producer-%d.msgs", i)
	// 	pubInterval := time.Duration(rand.Intn(1000)) * time.Millisecond
	// 	go sp.publishMessages(&wg, i, nc, subj, rand.Intn(100), pubInterval)
	// }

	sp := newPublisher("normal")
	id := 0
	subj := "trace_data.spec-a-1"
	pubInterval := time.Millisecond * 1
	msgCount := 1_000
	wg.Add(1)
	go sp.publishMessages(&wg, id, nc, subj, msgCount, pubInterval)

	slog.Info("Waiting for publishers to complete work...")
	wg.Wait()
	slog.Info("All publisher work completed")
}

func encodeFloat64Array(data []float64) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, v := range data {
		err := binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func newPublisher(pubType string) IPublisher {
	switch pubType {
	case "simple":
		return &SimpleTestPublisher{}
	case "normal":
		return &Publisher{}
	default:
		return &Publisher{}
	}
}
