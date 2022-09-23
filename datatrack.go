package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
)

type DatatrackNewBroadcastHubEvent struct {
	Sid string `json:"sid"`
}

func datatrackHandler(_ context.Context, msg *pubsub.Message) {
	if msg.Attributes["eventName"] != "new-broadcast-hub" {
		msg.Ack()
		return
	}
	log := logrus.WithField("msgId", msg.ID)

	msgProcessedSuccessfully := false

	recordableRooms.Inc()
	defer func() {
		defer recordableRooms.Dec()
		if msgProcessedSuccessfully {
			msg.Ack()
		} else {
			msg.Nack()
		}
		if r := recover(); r != nil {
			log.Errorf("Recovered: %v", r)
		}
	}()

	log.Infof("got message from datatrack: data=%v, attributes=%v, publishTime=%v", string(msg.Data), msg.Attributes, msg.PublishTime)

	var newBroadcastHub DatatrackNewBroadcastHubEvent
	if err := json.NewDecoder(bytes.NewReader(msg.Data)).Decode(&newBroadcastHub); err != nil {
		log.WithError(err).Errorf("cannot decode payload for event(new-broadcast-hub)")
		return
	}

	var datatrackAddress string
	if datatrackHostname, ok := msg.Attributes["hostname"]; ok {
		datatrackAddress = fmt.Sprintf("wss://%v/ws", datatrackHostname)
	} else {
		datatrackAddress = os.Getenv("DATATRACK_ADDRESS")
	}

	if done, err := runRecorder(context.Background(), log, datatrackAddress, newBroadcastHub.Sid); err != nil {
		log.WithError(err).WithField("ack", false).Errorf("cannot run recorder")
	} else {
		if done != nil {
			log.Info("recording is running")
			<-done
			log.Info("recording finished")
		} else {
			log.WithField("ack", true).Info("recording has not been run")
		}
		msgProcessedSuccessfully = true
	}
}
