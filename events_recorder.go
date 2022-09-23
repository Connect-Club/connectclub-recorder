package main

import (
	"connectclub-recorder/datatrack"
	"connectclub-recorder/pcap"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func runEventsRecorder(ctx context.Context, log *logrus.Entry, datatrackAddress, accessToken, roomId, roomPassword string) (string, <-chan struct{}, error) {
	client := datatrack.NewClient(datatrackAddress, accessToken, roomId, roomPassword)

	clientCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	userId, messages, startTime, err := client.Start(clientCtx)
	cancel()
	if err != nil {
		return "", nil, fmt.Errorf("can not start datatrack client, err=%w", err)
	}

	eventsFile := filepath.Join(roomId, fmt.Sprintf("events-%v", startTime.Unix()))
	saveMessagesDone := saveMessages(log, eventsFile, messages)

	done := make(chan struct{})

	go func() {
		defer close(done)

		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).Info("closing datatrack client due to canceled context")
			if err := client.Stop(); err != nil {
				log.WithError(err).Warn("cannot stop datatrack client")
			}
		case <-client.Stopped():
			log.Info("datatrack client self stopped")
		}

		select {
		case <-saveMessagesDone:
			log.Info("all events have been saved to file")
		case <-time.After(time.Minute):
			log.Panic("too long saving events to file")
		}

		select {
		case storageTasks <- StorageTask{log: log, filePath: eventsFile}:
			log.Info("storage task has been put to the queue")
		case <-time.After(time.Minute):
			log.Panic("too long putting storage task to the queue")
		}
	}()
	return userId, done, nil
}

func saveMessages(log *logrus.Entry, filePath string, messageCh <-chan *datatrack.CapturedMessage) <-chan struct{} {
	w, err := pcap.NewSimpleWriter(
		log.WithField("type", "events"),
		filepath.Join(".", recordsPath, filePath),
	)
	if err != nil {
		log.WithError(err).Panic("can not create writer for events")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range messageCh {
			//log.Info(msg)
			err := w.PushPacket(msg.CapturedTime, msg.RawMessage)
			if err != nil {
				log.WithError(err).Panic("can not push event packet to writer")
			}
		}
		w.NoMorePacket()
	}()
	return done
}

type Event struct {
	Time   int64
	UserId int
	Video  string
	State  string
	Move   string
}

func createEventsPack(log *logrus.Entry, recordsDirPath string) error {
	var startTime time.Time
	var eventFiles []string
	err := filepath.WalkDir(recordsDirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			matched, err := regexp.MatchString("^events-\\d*$", d.Name())
			if err != nil {
				log.WithError(err).Panic("regex error")
			}
			if matched {
				eventFiles = append(eventFiles, path)
				sec, err := strconv.ParseInt(strings.TrimPrefix(d.Name(), "events-"), 10, 0)
				if err != nil {
					log.WithError(err).Panic("can not parse start time")
				}
				if startTime.IsZero() {
					startTime = time.Unix(sec, 0)
				} else if startTime.Unix() > sec {
					startTime = time.Unix(sec, 0)
				}
			}
		}
		return nil
	})
	for _, eventFile := range eventFiles {
		reader, err := pcap.NewSimpleReader(log.WithField("", ""), eventFile)
		if err != nil {
			log.WithError(err).Panic("can not create simple reader")
		}
		for {
			var message *datatrack.Message
			capturedTime := reader.PopPacket(&message)
			if capturedTime.IsZero() {
				break
			}
			fmt.Println(message.Type)
			fmt.Println(string(message.Payload))
		}
	}
	return err
}
