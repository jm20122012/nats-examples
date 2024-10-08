package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

const (
	NATS_SERVER_URL string = "192.168.3.34:4222"
)

var (
	totalMsgcout int
)

// A sync subscriber requires the consumer to use NextMsg to read the next message from the buffer.
// The subscriber object still receives all messages from the server, but places them in a buffer until the
// subsciber object calls NextMsg.  Somewhat like a queue.
func createSyncSubscriber(nc *nats.Conn, subject string) (*nats.Subscription, error) {
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		return &nats.Subscription{}, err
	}
	return sub, nil
}

// An async subscriber consumes the data from the subject as soon as it's available.
func createAsyncSubscriber(nc *nats.Conn, subject string) (*nats.Subscription, error) {
	// handler := subMsgHandlerFactory(subject)
	handler := subMsgHandlerFactory()
	sub, err := nc.Subscribe(subject, handler)
	if err != nil {
		return &nats.Subscription{}, err
	}
	return sub, nil
}

func subMsgHandlerFactory() nats.MsgHandler {
	return func(msg *nats.Msg) {
		// slog.Info("Message received", "subject", subject, "msg", msg.Data)
		totalMsgcout++
	}
}

func main() {
	// Create a channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Register the channel to receive SIGINT signals
	signal.Notify(sigs, syscall.SIGINT)

	// Start a goroutine to handle the signal
	go func() {
		sig := <-sigs
		slog.Info("Signal received", "signal", sig)

		// Perform any cleanup here
		slog.Info("Exiting", "totalMessagesReceived", totalMsgcout)
		os.Exit(0)
	}()

	nc, err := nats.Connect(NATS_SERVER_URL)
	if err != nil {
		slog.Error("error connecting to NATS server", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	slog.Info("Connected to NATS server", "serverURL", NATS_SERVER_URL)

	// slog.Info("Creating sync consumer")
	// syncSub, err := createSyncSubscriber(nc, "producer-0.>")
	// if err != nil {
	// 	slog.Error("Error creating sync subscriber", "error", err)
	// }
	// defer syncSub.Drain()

	slog.Info("Creating async consumer")
	sub, err := createAsyncSubscriber(nc, "trace_data.>")
	if err != nil {
		slog.Error("Error creating async subscriber", "error", err)
	}
	defer sub.Drain()

	<-sigs

	// ticker := time.NewTicker(time.Duration(1 * time.Second))
	// defer ticker.Stop()

	// timer := time.NewTimer(time.Duration(60 * time.Second))
	// defer timer.Stop()
	// <-timer.C
	// for {
	// 	select {
	// 	case <-ticker.C:
	// 		m, err := syncSub.NextMsg(time.Duration(5 * time.Second))
	// 		if err != nil {
	// 			if errors.Is(err, nats.ErrTimeout) {
	// 				slog.Info("No messages in buffer for sync sub")
	// 			} else {
	// 				slog.Error("Could not get next message for sync sub", "error", err)
	// 			}
	// 			continue
	// 		}
	// 		slog.Info("New message on sync sub", "msg", m.Data)
	// 	case <-timer.C:
	// 		slog.Info("Timer expired - exiting main")
	// 		return
	// 	}
	// }
}
