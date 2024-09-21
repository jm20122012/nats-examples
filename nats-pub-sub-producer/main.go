package main

import (
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

func publishMessages(wg *sync.WaitGroup, id int, nc *nats.Conn, subject string, msgCount int, interval time.Duration) {
	defer wg.Done()
	slog.Info("Starting publisher go routine", "id", id, "msgCount", msgCount, "interval", interval)

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

func main() {
	nc, err := nats.Connect(NATS_SERVER_URL)
	if err != nil {
		slog.Error("error connecting to NATS server", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	slog.Info("Connected to NATS server", "serverURL", NATS_SERVER_URL)

	var wg sync.WaitGroup
	for i := 0; i < PRODUCER_COUNT; i++ {
		wg.Add(1)
		subj := fmt.Sprintf("producer-%d.msgs", i)
		pubInterval := time.Duration(rand.Intn(1000)) * time.Millisecond
		go publishMessages(&wg, i, nc, subj, rand.Intn(100), pubInterval)
	}

	slog.Info("Waiting for publishers to complete work...")
	wg.Wait()
	slog.Info("All publisher work completed")
}
