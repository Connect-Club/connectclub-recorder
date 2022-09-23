package main

import (
	"connectclub-recorder/utils"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

var accessToken string

func init() {
	var ok bool
	if accessToken, ok = os.LookupEnv("ACCESS_TOKEN"); !ok {
		accessToken = "service-access-token"
	}
}

func runRecorder(ctx context.Context, log *logrus.Entry, datatrackAddress, conferenceGid string) (<-chan struct{}, error) {
	log = log.WithField("conferenceGid", conferenceGid)

	recorderCtx, cancel := context.WithCancel(context.WithValue(ctx, "conferenceGid", conferenceGid))

	userId, eventsRecorderDone, eventsErr := runEventsRecorder(recorderCtx, log, datatrackAddress, accessToken, conferenceGid, "")
	if eventsErr != nil {
		cancel()
		if utils.GetRootError(eventsErr).Error() == "room-not-found" {
			log.WithError(eventsErr).Warn("room not found")
			return nil, nil
		} else {
			return nil, fmt.Errorf("can not start events recorder, err=%w", eventsErr)
		}
	}
	log.Infof("userId = %v", userId)
	mediaRecorderDone, mediaErr := runMediaRecorder(recorderCtx, log, userId, conferenceGid)
	if mediaErr != nil {
		cancel()
		return nil, fmt.Errorf("can not start media recorder, err=%w", mediaErr)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).Warn("closing recorder due to canceled context")
		case <-eventsRecorderDone:
			log.Info("closing due to completed recording of events")
			eventsRecorderDone = nil
		case <-mediaRecorderDone:
			log.Warn("closing due to completed recording of media")
			mediaRecorderDone = nil
		}
		cancel()

		timeout := time.After(10 * time.Second)
		for {
			select {
			case <-timeout:
				log.Panicf("close wait timeout(eventsRecorderDone=%v, mediaRecorderDone=%v)", eventsRecorderDone == nil, mediaRecorderDone == nil)
			case <-eventsRecorderDone:
				log.Info("events recorder closed")
				eventsRecorderDone = nil
			case <-mediaRecorderDone:
				log.Info("media recorder closed")
				mediaRecorderDone = nil
			}
			if eventsRecorderDone == nil && mediaRecorderDone == nil {
				break
			}
		}
	}()

	return done, nil
}
