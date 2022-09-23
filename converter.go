package main

import (
	"connectclub-recorder/pcap"
	"connectclub-recorder/utils"
	_ "embed"
	"fmt"
	"github.com/at-wat/ebml-go/webm"
	"github.com/pion/rtcp"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//go:embed webm/opus-silence-sample
var opusSilenceSampleData []byte

//go:embed webm/vp8-360x360-black-sample
var vp8BlackSampleData []byte

const (
	vp8BlackInterval    = time.Second / 5
	opusSilenceInterval = time.Second / 50
)

type sample struct {
	keyframe        bool
	time            time.Time
	b               []byte
	packetTimestamp uint32
	sr              *rtcp.SenderReport
}

type ByRtpTime []*rtcp.SenderReport

func (a ByRtpTime) Len() int           { return len(a) }
func (a ByRtpTime) Less(i, j int) bool { return a[i].RTPTime < a[j].RTPTime }
func (a ByRtpTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type track struct {
	ctxLog        *log.Entry
	startTime     time.Time
	alive         bool
	writer        webm.BlockWriteCloser
	sampleCh      chan *sample
	realSample    *sample
	sampleToWrite *sample

	lastWrittenSample *sample

	fakeSampleData     []byte
	fakeSampleInterval time.Duration
}

func (t *track) populateSample() bool {
	if !t.alive {
		return false
	} else if t.sampleToWrite != nil {
		return true
	}

	if t.realSample == nil {
		t.realSample, t.alive = <-t.sampleCh
		if !t.alive {
			return false
		}
	}

	var timestamp time.Time
	if t.lastWrittenSample == nil {
		timestamp = t.startTime
	} else {
		timestamp = t.lastWrittenSample.time.Add(t.fakeSampleInterval)
	}

	if t.fakeSampleData != nil && t.realSample.keyframe && t.realSample.time.Sub(timestamp) > t.fakeSampleInterval {
		t.ctxLog.Trace("generate fake sample")
		t.sampleToWrite = &sample{
			keyframe: true,
			time:     timestamp,
			b:        t.fakeSampleData,
		}
	} else {
		t.ctxLog.Trace("process real sample")
		if t.lastWrittenSample != nil && !t.realSample.time.After(t.lastWrittenSample.time) {
			oldTime := t.realSample.time
			t.realSample.time = t.lastWrittenSample.time.Add(time.Millisecond)
			t.ctxLog.Infof("sample time fixed from %v to %v", oldTime.Sub(t.startTime), t.realSample.time.Sub(t.startTime))
		}
		t.sampleToWrite = t.realSample
		t.realSample = nil
	}

	return true
}

func findClosestSenderReport(senderReports []*rtcp.SenderReport, startPosition int, packetTimestamp uint32) (position int, senderReport *rtcp.SenderReport) {
	var left, right *rtcp.SenderReport
	if startPosition == -1 {
		left, right = nil, senderReports[0]
	} else if startPosition == len(senderReports)-1 {
		left, right = senderReports[startPosition], nil
	} else {
		left, right = senderReports[startPosition], senderReports[startPosition+1]
	}

	position = startPosition

	for {
		if right != nil && packetTimestamp > right.RTPTime {
			position++
			left = right
			if position == len(senderReports)-1 {
				right = nil
			} else {
				right = senderReports[position+1]
			}
		} else if left != nil && packetTimestamp < left.RTPTime {
			position--
			right = left
			if position == -1 {
				left = nil
			} else {
				left = senderReports[position]
			}
		} else {
			break
		}
	}

	if left == nil {
		senderReport = right
	} else if right == nil {
		senderReport = left
	} else {
		if packetTimestamp-left.RTPTime < right.RTPTime-packetTimestamp {
			senderReport = left
		} else {
			senderReport = right
		}
	}
	return
}

func packetTimestampToTime(packetTimestamp uint32, senderReports []*rtcp.SenderReport, srStartPosition int, sampleRate uint32, timeFix time.Duration) (packetTime time.Time, sr *rtcp.SenderReport, srPosition int) {
	srPosition, sr = findClosestSenderReport(senderReports, srStartPosition, packetTimestamp)

	samplesSinceSr := int64(packetTimestamp) - int64(sr.RTPTime)
	durationSinceSr := time.Duration(int64(time.Second) * samplesSinceSr / int64(sampleRate))
	packetTime = utils.NtpTime(sr.NTPTime).Time().
		Add(durationSinceSr).
		Add(timeFix)
	return
}

func processTrack(ctxLog *log.Entry, reader *pcap.Reader, trackUID uint64, sampleRate uint32, isKeyframe func([]byte) bool) chan *sample {
	var senderReports []*rtcp.SenderReport
	var timeFix time.Duration
	timeFixSet := false
	for {
		rtcpPackets, capturedTime := reader.PopRTCP(trackUID)
		if rtcpPackets == nil {
			break
		}
		for _, rtcpPacket := range rtcpPackets {
			senderReport, ok := rtcpPacket.(*rtcp.SenderReport)
			if !ok {
				continue
			}
			timeFixCandidate := capturedTime.Sub(utils.NtpTime(senderReport.NTPTime).Time())
			if !timeFixSet || math.Abs(float64(timeFixCandidate)) < math.Abs(float64(timeFix)) {
				timeFix = timeFixCandidate
				timeFixSet = true
			}
			senderReports = append(senderReports, senderReport)
		}
	}
	sort.Sort(ByRtpTime(senderReports))
	srPosition := -1

	sampleCh := make(chan *sample)
	//lastPacketTime, _, _ := packetTimestampToTime(lastTimestamp, senderReports, 0, sampleRate, timeFix)
	go func() {
		defer close(sampleCh)

		for {
			s, _ := reader.PopSample(trackUID)
			if s == nil {
				break
			}

			var ts time.Time
			var sr *rtcp.SenderReport
			ts, sr, srPosition = packetTimestampToTime(s.PacketTimestamp, senderReports, srPosition, sampleRate, timeFix)

			sampleCh <- &sample{
				keyframe:        isKeyframe(s.Data),
				time:            ts,
				b:               s.Data,
				sr:              sr,
				packetTimestamp: s.PacketTimestamp,
			}
		}
	}()
	return sampleCh
}

func createWebm(ctxLog *log.Entry, srcDirPath string) string {
	reader, err := pcap.NewReader(
		ctxLog,
		filepath.Join(".", recordsPath, srcDirPath),
	)
	if err != nil {
		ctxLog.WithError(err).Panic("can not create stream reader")
	}

	webmTracks := make([]webm.TrackEntry, 0, len(reader.Meta.Tracks))
	tracks := make([]*track, 0, len(reader.Meta.Tracks))
	for i, metaTrack := range reader.Meta.Tracks {
		webmTrack := webm.TrackEntry{
			TrackUID:    metaTrack.Uid,
			TrackNumber: uint64(i + 1),
		}
		var trackKind string
		var isKeyframeFn func([]byte) bool
		var fakeSampleData []byte
		var fakeSampleInterval time.Duration
		if metaTrack.Audio != nil {
			trackKind = "audio"
			isKeyframeFn = func([]byte) bool { return true }
			webmTrack.Name = fmt.Sprintf("Audio_%v", metaTrack.Uid)
			webmTrack.CodecID = "A_OPUS"
			webmTrack.TrackType = 2
			webmTrack.Audio = &webm.Audio{
				SamplingFrequency: float64(metaTrack.SampleRate),
				Channels:          metaTrack.Audio.Channels,
			}
			fakeSampleData = opusSilenceSampleData
			fakeSampleInterval = opusSilenceInterval
		} else if metaTrack.Video != nil {
			trackKind = "video"
			isKeyframeFn = func(d []byte) bool { return d[0]&0x1 == 0 }
			webmTrack.Name = fmt.Sprintf("Video_%v", metaTrack.Uid)
			webmTrack.CodecID = "V_VP8"
			webmTrack.TrackType = 1
			webmTrack.Video = &webm.Video{
				PixelWidth:  metaTrack.Video.Width,
				PixelHeight: metaTrack.Video.Height,
			}
			fakeSampleData = vp8BlackSampleData
			fakeSampleInterval = vp8BlackInterval
		} else {
			ctxLog.Panic("no audio or video set")
		}
		ctxLog := ctxLog.WithField("kind", trackKind).WithField("uid", metaTrack.Uid)
		sampleCh := processTrack(
			ctxLog,
			reader,
			metaTrack.Uid,
			metaTrack.SampleRate,
			isKeyframeFn,
		)
		track := track{
			ctxLog:             ctxLog,
			startTime:          reader.Meta.StartTime,
			alive:              true,
			sampleCh:           sampleCh,
			fakeSampleData:     fakeSampleData,
			fakeSampleInterval: fakeSampleInterval,
		}

		webmTracks = append(webmTracks, webmTrack)
		tracks = append(tracks, &track)
	}

	webmFile, err := os.Create(filepath.Join(".", recordsPath, srcDirPath+".webm"))
	if err != nil {
		ctxLog.WithError(err).Panic("can not create webm file")
	}

	simpleBlockWriters, err := webm.NewSimpleBlockWriter(webmFile, webmTracks)
	if err != nil {
		ctxLog.WithError(err).Panic("can not create webm block writer")
	}

	for i, simpleBlockWriter := range simpleBlockWriters {
		tracks[i].writer = simpleBlockWriter
	}

	for {
		aliveTracks := make([]*track, 0, len(tracks))
		for _, t := range tracks {
			if t.populateSample() {
				aliveTracks = append(aliveTracks, t)
			} else {
				t.ctxLog.Infof("no more samples")
				if err := t.writer.Close(); err != nil {
					t.ctxLog.WithError(err).Warn("can not close webm block writer")
				}
			}
		}
		if len(aliveTracks) == 0 {
			ctxLog.Info("all tracks are dead")
			break
		}
		tracks = aliveTracks
		sort.Slice(tracks, func(i, j int) bool {
			return tracks[i].sampleToWrite.time.Before(tracks[j].sampleToWrite.time)
		})

		t := tracks[0]
		if _, err := t.writer.Write(t.sampleToWrite.keyframe, int64(t.sampleToWrite.time.Sub(reader.Meta.StartTime)/time.Millisecond), t.sampleToWrite.b); err != nil {
			t.ctxLog.WithError(err).Warn("can not write sample")
		} else {
			t.ctxLog.Tracef("sample has been written, ts=%v, keyframe=%v", t.sampleToWrite.time.Sub(reader.Meta.StartTime), t.sampleToWrite.keyframe)
			if t.lastWrittenSample != nil && t.lastWrittenSample.packetTimestamp != 0 && t.sampleToWrite.packetTimestamp != 0 && t.lastWrittenSample.packetTimestamp > t.sampleToWrite.packetTimestamp {
				t.ctxLog.Warnf("wrong timestamp order, lastWrittenSample.packetTimestamp=%v, sampleToWrite.packetTimestamp=%v", t.lastWrittenSample.packetTimestamp, t.sampleToWrite.packetTimestamp)
			}
			if t.lastWrittenSample != nil && t.lastWrittenSample.time.After(t.sampleToWrite.time) {
				t.ctxLog.Warnf("wrong time order, lastWrittenSample.time=%v, sampleToWrite.time=%v", t.lastWrittenSample.time.Sub(reader.Meta.StartTime), t.sampleToWrite.time.Sub(reader.Meta.StartTime))
			}
			t.lastWrittenSample = t.sampleToWrite
		}
		t.sampleToWrite = nil
	}
	return srcDirPath + ".webm"
}
