package main

import (
	jvbuster "connectclub-recorder/jvbuster_wrapper"
	"connectclub-recorder/pcap"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt"
	"github.com/pion/rtcp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v3"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	vp8TrackUID  = 12345
	opusTrackUID = 67890
)

func parseRsaPrivateKeyFromPemStr(privPEM []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(privPEM)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	privKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return privKey, nil
}

func runMediaRecorder(ctx context.Context, log *logrus.Entry, endpoint, conferenceGid string) (<-chan struct{}, error) {
	privPEM, err := base64.StdEncoding.DecodeString(os.Getenv("JVBUSTER_KEY"))
	if err != nil {
		log.WithError(err).Panic("can not parse private key")
	}
	privKey, err := parseRsaPrivateKeyFromPemStr(privPEM)
	if err != nil {
		log.WithError(err).Panic("can not parse private key")
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"endpoint":      endpoint,
		"conferenceGid": conferenceGid,
	}).SignedString(privKey)
	if err != nil {
		return nil, fmt.Errorf("can not sign token, err = %w", err)
	}

	client := jvbuster.NewClient(
		log,
		os.Getenv("JVBUSTER_ADDRESS"),
		token,
		nil,
		nil,
	)

	clientCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	streams, err := client.Start(clientCtx, false)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("can not start jvbuster client, err = %w", err)
	}

	saveStreamsDone := saveStreams(log, conferenceGid, streams)

	done := make(chan struct{})

	go func() {
		defer close(done)

		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).Info("closing jvbuster client due to canceled context")
			if err = client.Stop(); err != nil {
				log.WithError(err).Warn("can not stop jvbuster client")
			}
		case <-client.Stopped():
			log.Warn("jvbuster client self stopped")
		}

		<-saveStreamsDone
	}()

	return done, nil
}

func processTrackFuture(log *logrus.Entry, pcapWriter *pcap.Writer, trackUID uint64, trackFuture *jvbuster.TrackRemoteFuture) {
	if trackFuture == nil {
		log.Panic("trackFuture == nil")
	}

	log = log.WithField("kind", trackFuture.Kind().String())
	trackFuture.OnDone(func(track *webrtc.TrackRemote, rtpReceiver *webrtc.RTPReceiver) {
		if track == nil {
			log.Info("empty track received")
			pcapWriter.NoMoreRTP(trackUID)
			pcapWriter.NoMoreRTCP(trackUID)
			return
		}
		log = log.WithField("trackId", track.ID())
		log.Info("track received")

		go func() {
			firstRtpRed := false
			for {
				rtpPacket, _, readErr := track.ReadRTP()
				capturedTime := time.Now()
				if readErr != nil {
					if readErr != io.EOF {
						log.WithError(readErr).Warn("can not read RTP data from track")
					} else {
						log.Info("reached RTP EOF")
					}
					pcapWriter.NoMoreRTP(trackUID)
					return
				} else if !firstRtpRed {
					log.Info("first RTP red")
					firstRtpRed = true
				}
				pcapWriter.PushRTP(trackUID, capturedTime, rtpPacket)
			}
		}()

		go func() {
			firstRtcpRed := false
			for {
				rtcpPackets, _, readErr := rtpReceiver.ReadRTCP()
				capturedTime := time.Now()
				if readErr != nil {
					if readErr != io.EOF {
						log.WithError(readErr).Warn("can not read RTCP data from track")
					} else {
						log.Info("reached RTCP EOF")
					}
					pcapWriter.NoMoreRTCP(trackUID)
					return
				} else if !firstRtcpRed {
					log.Info("first RTCP red")
					firstRtcpRed = true
				}
				pcapWriter.PushRTCP(trackUID, capturedTime, rtcpPackets)
			}
		}()
	})
}

var tracksInfo = []pcap.TrackWriterInfo{
	{
		TrackUID:     vp8TrackUID,
		Depacketizer: &codecs.VP8Packet{},
		SampleRate:   90000,
	}, {
		TrackUID:     opusTrackUID,
		Depacketizer: &codecs.OpusPacket{},
		SampleRate:   48000,
	},
}

func saveStreams(log *logrus.Entry, path string, streams <-chan jvbuster.StreamRemote) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		saveStreamDones := make([]<-chan struct{}, 0)
		for stream := range streams {
			saveStreamDones = append(saveStreamDones, saveStream(log, path, stream))
		}
		for _, saveStreamDone := range saveStreamDones {
			<-saveStreamDone
		}
	}()
	return done
}

func saveStream(log *logrus.Entry, dirPath string, stream jvbuster.StreamRemote) <-chan struct{} {
	log = log.WithFields(logrus.Fields{
		"streamId":        stream.Id,
		"streamTimestamp": stream.StartTime.Unix(),
	})
	log.Info("onStream")

	streamPath := filepath.Join(dirPath, fmt.Sprintf("%s-%d", stream.Id, stream.StartTime.Unix()))
	pcapWriter, err := pcap.NewWriter(
		log,
		stream.StartTime,
		filepath.Join(".", recordsPath, streamPath),
		tracksInfo,
	)
	if err != nil {
		log.WithError(err).Panic("can not create stream writer")
	}

	processTrackFuture(log, pcapWriter, opusTrackUID, stream.AudioTrackFuture)
	processTrackFuture(log, pcapWriter, vp8TrackUID, stream.VideoTrackFuture)

	go func() {
		for keyFrameRequest := range pcapWriter.KeyframeRequests {
			errSend := stream.PeerConnection.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: keyFrameRequest.SSRC}})
			if errSend != nil {
				log.WithField("err", errSend).Warn("send PLI error")
			} else {
				log.Trace("PLI sent")
			}
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-pcapWriter.Done()
		streamParts, _ := ioutil.ReadDir(filepath.Join(".", recordsPath, streamPath))
		for _, file := range streamParts {
			if file.IsDir() {
				continue
			}
			storageTasks <- StorageTask{
				log:      log,
				filePath: filepath.Join(streamPath, file.Name()),
			}
		}
		//storageTasks <- StorageTask{
		//	log: log,
		//	filePath: createWebm(log, streamPath),
		//}
	}()
	return done
}
