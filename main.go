package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

//func main0() {
//	createEventsPack("./records")
//}

const recordsPath = "records"

var (
	recordableRooms = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "recorder_recordable_rooms",
	})
)

func runHttp() {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.WithError(err).Error("cannot start http server")
	}
}

func main() {
	log.SetReportCaller(true)
	log.SetFormatter(&log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
		FieldMap: log.FieldMap{
			log.FieldKeyTime: "@timestamp",
			log.FieldKeyMsg:  "message",
		},
	})

	go runHttp()

	dir, err := ioutil.ReadDir(filepath.Join(".", recordsPath))
	if err != nil {
		log.WithError(err).Panicf("cannot read directory `%v`", recordsPath)
	}
	for _, d := range dir {
		p := filepath.Join(".", recordsPath, d.Name())
		if err := os.RemoveAll(p); err != nil {
			log.WithError(err).Errorf("cannot remove path `%v`", p)
		}
	}

	runStorageTasksProcessing()

	projectId := os.Getenv("GCLOUD_PROJECT_ID")
	if projectId == "" {
		log.Panic("environment variable GCLOUD_PROJECT_ID is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	client, err := pubsub.NewClient(ctx, projectId)
	cancel()
	if err != nil {
		log.WithError(err).Panicf("can not connect to pubsub")
	}

	subId := os.Getenv("SUBSCRIPTION_ID")
	if subId == "" {
		subId = "recorder"
	}
	sub := client.Subscription(subId)
	sub.ReceiveSettings.Synchronous = true
	sub.ReceiveSettings.MaxOutstandingMessages = 1
	sub.ReceiveSettings.MaxExtension = time.Hour * 4
	sub.ReceiveSettings.MaxExtensionPeriod = time.Second * 5

	ctx, cancel = context.WithCancel(context.Background())

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		s := <-signals
		log.Infof("signal(%v) received, will stop soon", s)
		cancel()
	}()

	log.Infof("starting receive messages from %v", sub)
	if err := sub.Receive(ctx, datatrackHandler); err != nil {
		log.WithError(err).Error("pubsub subscription stopped abnormally")
	} else {
		log.Info("pubsub subscription stopped")
	}

	close(storageTasks)
	storageWG.Wait()
	log.Info("recorder stopped")
}
