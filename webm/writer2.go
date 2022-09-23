package webm

import (
	"connectclub-recorder/utils"
	_ "embed"
	"fmt"
	"github.com/at-wat/ebml-go/webm"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v3/pkg/media"
	"github.com/pion/webrtc/v3/pkg/media/samplebuilder"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"sort"
	"time"
)

//go:embed vp8-360x360-black-sample
var vp8BlackSampleData []byte

//go:embed opus-silence-sample
var opusSilenceSampleData []byte

const (
	vp8BlackInterval    = time.Second / 5
	opusSilenceInterval = time.Second / 50
)

type KeyframeRequest struct {
	TrackUID uint64
	SSRC     uint32
}

type Track2 struct {
	ctxLog                  *log.Entry
	writer                  webm.BlockWriteCloser
	sampleBuilder           *samplebuilder.SampleBuilder
	sampleBuilderLastSample *media.Sample
	packetFirstTs           uint32
	packetFirstTime         time.Time
	sampleCh                chan *media.Sample
	senderReportCh          chan *rtcp.SenderReport
	senderReport            *rtcp.SenderReport
	sampleRate              uint32
	ssrc                    uint32
	webmEntry               webm.TrackEntry

	alive         bool
	realSample    *media.Sample
	sampleToWrite *media.Sample
	//wasFakeSample     bool
	lastWrittenSample *media.Sample
	needKeyframe      bool

	samplesWritten int
}

type Writer2 struct {
	ctxLog               *log.Entry
	tracks               map[uint64]*Track2
	start                time.Time
	deviceTimeCorrection time.Duration
	KeyframeRequests     chan KeyframeRequest
}

func NewWriter2(filePath string, ctxLog *log.Entry, tracks []webm.TrackEntry) (*Writer2, error) {
	webmFile, err := os.OpenFile(filePath+".webm", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("can not create webm file: %w", err)
	}

	writers, err := webm.NewSimpleBlockWriter(webmFile, tracks)
	if err != nil {
		return nil, fmt.Errorf("can not create webm block writer: %w", err)
	}

	w := Writer2{
		ctxLog:           ctxLog,
		tracks:           make(map[uint64]*Track2),
		start:            time.Now(),
		KeyframeRequests: make(chan KeyframeRequest, 1024),
	}

	for i, t := range tracks {
		var sampleRate uint32
		var depacketizer rtp.Depacketizer
		if t.Audio != nil {
			sampleRate = uint32(t.Audio.SamplingFrequency)
			depacketizer = &codecs.OpusPacket{}
		} else if t.Video != nil {
			sampleRate = 90000
			depacketizer = &codecs.VP8Packet{}
		} else {
			panic("non audio non video")
		}
		webmTrack := &Track2{
			webmEntry:      t,
			writer:         writers[i],
			sampleBuilder:  samplebuilder.New(1000, depacketizer, sampleRate),
			sampleCh:       make(chan *media.Sample, int64(math.Pow(2, 16))),
			senderReportCh: make(chan *rtcp.SenderReport, int64(math.Pow(2, 16))),
			sampleRate:     sampleRate,
			alive:          true,
			ctxLog:         w.ctxLog.WithField("trackName", t.Name).WithField("trackUID", t.TrackUID),
			needKeyframe:   true,
		}
		w.tracks[t.TrackUID] = webmTrack
	}

	go w.loop()

	return &w, nil
}

func (w *Writer2) PushRTP(trackUID uint64, rtpPacket *rtp.Packet) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track name")
		return
	}

	ctxLog = track.ctxLog
	if track.ssrc == 0 {
		track.ssrc = rtpPacket.SSRC
		track.packetFirstTs = rtpPacket.Timestamp
		track.packetFirstTime = time.Now()
		ctxLog.Info("first RTP packet received")
	} else if track.ssrc != rtpPacket.SSRC {
		ctxLog.Error("SSRC changed")
		return
	}

	track.sampleBuilder.Push(rtpPacket)

	for {
		sample := track.sampleBuilder.Pop()
		if sample == nil {
			break
		}
		ctxLog.Trace("sample builder produced sample")

		logKeyframe := false
		keyFrame := true
		if track.webmEntry.Video != nil {
			keyFrame = sample.Data[0]&0x1 == 0
			logKeyframe = true
		}

		if keyFrame {
			if logKeyframe {
				raw := uint(sample.Data[6]) | uint(sample.Data[7])<<8 | uint(sample.Data[8])<<16 | uint(sample.Data[9])<<24
				width := int(raw & 0x3FFF)
				height := int((raw >> 16) & 0x3FFF)
				ctxLog.Infof("keyframe received (width=%v, height=%v, timestamp=%v)", width, height, sample.PacketTimestamp)
			}
			track.needKeyframe = false
		} else if sample.PrevDroppedPackets > 0 {
			if logKeyframe {
				ctxLog.Infof("needs keyframe, dropped packets = %v", sample.PrevDroppedPackets)
			}
			track.needKeyframe = true
		}
		//else if track.sampleBuilderLastSample != nil && (sample.PacketTimestamp-track.sampleBuilderLastSample.PacketTimestamp) > track.sampleRate / 2 {
		//	if logKeyframe {
		//		ctxLog.Infof("needs keyframe, too high timestamp delta - %v", sample.PacketTimestamp-track.sampleBuilderLastSample.PacketTimestamp)
		//	}
		//	track.needKeyframe = true
		//}
		if track.needKeyframe {
			w.KeyframeRequests <- KeyframeRequest{
				TrackUID: track.webmEntry.TrackUID,
				SSRC:     track.ssrc,
			}
		} else {
			track.sampleCh <- sample
		}
		track.sampleBuilderLastSample = sample
	}
}

func (w *Writer2) PushRTCP(trackUID uint64, rtcpPackets []rtcp.Packet) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track name")
		return
	}

	ctxLog = track.ctxLog
	for _, rtcpPacket := range rtcpPackets {
		senderReport, ok := rtcpPacket.(*rtcp.SenderReport)
		if !ok || senderReport.SSRC != track.ssrc {
			continue
		}
		//if w.startNTP == 0 && track.packetFirstTs != 0 {
		//	samples := int64(senderReport.RTPTime) - int64(track.packetFirstTs)
		//	d := time.Duration(int64(time.Second) * samples / int64(track.sampleRate))
		//	w.startNTP = utils.NtpTime(senderReport.NTPTime).DurationSinceEpoch() - d
		//}
		if w.deviceTimeCorrection == 0 && track.packetFirstTs != 0 {
			samplesCount := int64(senderReport.RTPTime) - int64(track.packetFirstTs)
			samplesDuration := time.Duration(int64(time.Second) * samplesCount / int64(track.sampleRate))
			deviceTime := utils.NtpTime(senderReport.NTPTime).Time().Add(-samplesDuration)
			w.deviceTimeCorrection = track.packetFirstTime.Sub(deviceTime)
		}
		track.senderReportCh <- senderReport
	}
}

func (w *Writer2) NoMoreRTP(trackUID uint64) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track name")
		return
	}

	close(track.sampleCh)
}

func (w *Writer2) NoMoreRTCP(trackUID uint64) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track name")
		return
	}

	close(track.senderReportCh)
}

type BySampleTimestamp []*Track2

func (a BySampleTimestamp) Len() int      { return len(a) }
func (a BySampleTimestamp) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a BySampleTimestamp) Less(i, j int) bool {
	if a[i].sampleToWrite == nil && a[j].sampleToWrite == nil {
		return i < j
	}
	if a[i].sampleToWrite != nil && a[j].sampleToWrite == nil {
		return true
	}
	if a[i].sampleToWrite == nil && a[j].sampleToWrite != nil {
		return false
	}
	return a[i].sampleToWrite.Timestamp.Before(a[j].sampleToWrite.Timestamp)
}

func getDurationSinceSr(sample *media.Sample, sr *rtcp.SenderReport, sampleRate int64) time.Duration {
	samplesSinceSr := int64(sample.PacketTimestamp) - int64(sr.RTPTime)
	return time.Duration(int64(time.Second) * samplesSinceSr / sampleRate)
}

func (t *Track2) pullSample(w *Writer2) bool {
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

		var durationSinceSr time.Duration
		for {
			if t.senderReport != nil {
				durationSinceSr = getDurationSinceSr(t.realSample, t.senderReport, int64(t.sampleRate))
				if durationSinceSr < 10*time.Second {
					break
				}
			}
			t.ctxLog.Trace("waiting new sr")
			senderReport, ok := <-t.senderReportCh
			if ok {
				t.senderReport = senderReport
			} else {
				t.ctxLog.Info("no more sr, using the last one")
				break
			}
		}

		for w.deviceTimeCorrection == 0 {
			t.ctxLog.Info("waiting deviceTimeCorrection")
			time.Sleep(time.Second)
		}
		t.realSample.Timestamp = utils.NtpTime(t.senderReport.NTPTime).Time().Add(durationSinceSr + w.deviceTimeCorrection)
		t.ctxLog.Tracef("sample received timestamp=%v", t.realSample.Timestamp)
	}

	var fakeSampleInterval time.Duration
	var fakeSampleData []byte
	if t.webmEntry.Video != nil {
		fakeSampleInterval, fakeSampleData = vp8BlackInterval, vp8BlackSampleData
	} else if t.webmEntry.Audio != nil {
		fakeSampleInterval, fakeSampleData = opusSilenceInterval, opusSilenceSampleData
	} else {
		t.ctxLog.Panic("unknown track kind")
	}

	var timestamp time.Time
	if t.lastWrittenSample == nil {
		timestamp = w.start
	} else {
		timestamp = t.lastWrittenSample.Timestamp.Add(fakeSampleInterval)
	}

	fakeSampleAllow := true
	if t.webmEntry.Video != nil && t.lastWrittenSample != nil {
		fakeSampleAllow = false
	}

	if fakeSampleAllow && t.realSample.Timestamp.Sub(timestamp) > fakeSampleInterval {
		t.ctxLog.Trace("generate fake sample")
		t.sampleToWrite = &media.Sample{
			Timestamp: timestamp,
			Data:      fakeSampleData,
		}
	} else {
		t.ctxLog.Trace("process real sample")
		t.sampleToWrite = t.realSample
		t.realSample = nil
	}

	return true
}

func (w *Writer2) loop() {
	tracks := make([]*Track2, 0, len(w.tracks))
	for _, track := range w.tracks {
		tracks = append(tracks, track)
	}

	for w.start.IsZero() {
		time.Sleep(time.Second)
	}

	for {
		aliveTracks := make([]*Track2, 0, len(tracks))
		for _, t := range tracks {
			if t.pullSample(w) {
				aliveTracks = append(aliveTracks, t)
			} else {
				t.ctxLog.Infof("no more samples, samples per second = %v", float64(t.samplesWritten)/time.Now().Sub(w.start).Seconds())
			}
		}
		if len(aliveTracks) == 0 {
			w.ctxLog.Info("all tracks are dead")
			return
		}
		tracks = aliveTracks
		sort.Sort(BySampleTimestamp(tracks))

		t := tracks[0]
		keyFrame := true
		if t.webmEntry.Video != nil {
			keyFrame = t.sampleToWrite.Data[0]&0x1 == 0
		}
		if _, err := t.writer.Write(keyFrame, int64(t.sampleToWrite.Timestamp.Sub(w.start)/time.Millisecond), t.sampleToWrite.Data); err != nil {
			t.ctxLog.WithError(err).Warn("can not write sample")
		} else {
			t.ctxLog.Tracef("sample has been written, ts=%v", t.sampleToWrite.Timestamp.Sub(w.start))
			t.samplesWritten++
			t.lastWrittenSample = t.sampleToWrite
		}
		t.sampleToWrite = nil
	}
}
