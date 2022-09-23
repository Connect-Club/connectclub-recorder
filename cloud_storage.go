package main

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"sync"
)

var storageTasks chan StorageTask
var storageWG sync.WaitGroup

type StorageTask struct {
	log      *logrus.Entry
	filePath string
}

const storageTaskProcessors = 4

func runStorageTasksProcessing() {
	recordsBucket, isEnvSet := os.LookupEnv("RECORDS_BUCKET")
	if !isEnvSet {
		logrus.Panic("environment variable RECORDS_BUCKET not set")
	}
	logrus.Infof("RECORDS_BUCKET=%v", recordsBucket)

	storageTasks = make(chan StorageTask, 1024)
	ctx := context.Background()
	for i := 0; i < storageTaskProcessors; i++ {
		go processTasks(ctx, recordsBucket)
	}
}

func processTasks(ctx context.Context, recordsBucket string) {
	storageWG.Add(1)
	defer storageWG.Done()

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logrus.WithError(err).Panic("can not create google cloud client")
	}
	defer storageClient.Close()

	for storageTask := range storageTasks {
		processTask(ctx, storageClient, storageTask, recordsBucket)
	}
}

func processTask(ctx context.Context, storageClient *storage.Client, storageTask StorageTask, recordsBucket string) {
	log := storageTask.log
	log.Infof("Uploading file %v", storageTask.filePath)
	defer log.Infof("File %v uploaded", storageTask.filePath)

	file := filepath.Join(".", recordsPath, storageTask.filePath)
	defer os.Remove(file)
	if data, err := os.ReadFile(file); err != nil {
		log.WithError(err).Errorf("can not read data from file(%v)", storageTask.filePath)
	} else {
		object := storageClient.Bucket(recordsBucket).Object(storageTask.filePath)
		objectWriter := object.NewWriter(ctx)
		if _, err := objectWriter.Write(data); err != nil {
			log.WithError(err).Errorf("can not write data to object(%v)", storageTask.filePath)
		}
		if err := objectWriter.Close(); err != nil {
			log.WithError(err).Errorf("can not close object(%v)", storageTask.filePath)
		}
	}
}
