package webm

import (
	"connectclub-recorder/utils"
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
	"time"
)

const (
	vp8SampleRate  = 90000
	opusSampleRate = 48000
)

type Signal struct{}

var SignalInstance Signal

type Writer struct {
	VideoKeyframeNeeded chan Signal

	audioWriters, videoWriters               []webm.BlockWriteCloser
	audioSampleBuilder, videoSampleBuilder   *samplebuilder.SampleBuilder
	audioPacketFirstTs, videoPacketFirstTs   uint32
	audioPacketFirstNTP, videoPacketFirstNTP time.Duration
	audioPacketLastTs, videoPacketLastTs     int32
	audioTimestamp, videoTimestamp           time.Duration
	audioSampleCh, videoSampleCh             chan *media.Sample
	audioSSRC, videoSSRC                     uint32
	audioSr, videoSr                         *rtcp.SenderReport
	ctxLog                                   *log.Entry
}

func NewWriter(filePath string, ctxLog *log.Entry) *Writer {
	webmFile, err := os.OpenFile(filePath+".webm", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		ctxLog.WithError(err).Panic("can not create webm file")
	}

	blockWriters, err := webm.NewSimpleBlockWriter(webmFile,
		[]webm.TrackEntry{
			{
				Name:            "Video",
				TrackNumber:     1,
				TrackUID:        12345,
				CodecID:         "V_VP8",
				TrackType:       1,
				DefaultDuration: 33333333,
				Video: &webm.Video{
					PixelWidth:  uint64(360),
					PixelHeight: uint64(360),
				},
			}, {
				Name:            "Audio",
				TrackNumber:     2,
				TrackUID:        67890,
				CodecID:         "A_OPUS",
				TrackType:       2,
				DefaultDuration: 20000000,
				Audio: &webm.Audio{
					SamplingFrequency: 48000.0,
					Channels:          2,
				},
			},
		})
	if err != nil {
		ctxLog.WithError(err).Panic("can not create webm block writer")
	}

	writer := &Writer{
		VideoKeyframeNeeded: make(chan Signal, 1024),
		videoWriters:        []webm.BlockWriteCloser{blockWriters[0]},
		audioWriters:        []webm.BlockWriteCloser{blockWriters[1]},
		videoSampleBuilder:  samplebuilder.New(1000, &codecs.VP8Packet{}, vp8SampleRate, samplebuilder.WithMaxTimeDelay(5*time.Second)),
		audioSampleBuilder:  samplebuilder.New(1000, &codecs.OpusPacket{}, opusSampleRate, samplebuilder.WithMaxTimeDelay(5*time.Second)),
		audioSampleCh:       make(chan *media.Sample, 1024),
		videoSampleCh:       make(chan *media.Sample, 1024),
		ctxLog:              ctxLog,
	}

	go writer.loop()

	return writer
}

func (w *Writer) PushVP8(rtpPacket *rtp.Packet) {
	if w.videoSSRC == 0 {
		w.videoSSRC = rtpPacket.SSRC
		w.videoPacketFirstTs = rtpPacket.Timestamp
	} else if w.videoSSRC != rtpPacket.SSRC {
		w.ctxLog.Warn("video SSRC changed")
	}
	w.videoSampleBuilder.Push(rtpPacket)

	for {
		sample := w.videoSampleBuilder.Pop()
		if sample == nil {
			break
		}
		if sample.PrevDroppedPackets > 10 {
			w.ctxLog.Warn(fmt.Sprintf("dropped vp8 packets %d", sample.PrevDroppedPackets))
		}
		w.videoSampleCh <- sample
	}
}

func (w *Writer) CloseVP8() {
	close(w.videoSampleCh)
}

func (w *Writer) PushOpus(rtpPacket *rtp.Packet) {
	if w.audioSSRC == 0 {
		w.audioSSRC = rtpPacket.SSRC
		w.audioPacketFirstTs = rtpPacket.Timestamp
	} else if w.audioSSRC != rtpPacket.SSRC {
		w.ctxLog.Warn("audio SSRC changed")
	}
	w.audioSampleBuilder.Push(rtpPacket)

	for {
		sample := w.audioSampleBuilder.Pop()
		if sample == nil {
			break
		}
		if sample.PrevDroppedPackets > 10 {
			w.ctxLog.Warn(fmt.Sprintf("dropped opus packets %d", sample.PrevDroppedPackets))
		}
		w.audioSampleCh <- sample
	}
}

func (w *Writer) CloseOpus() {
	close(w.audioSampleCh)
}

func (w *Writer) PushVideoRtcp(rtcpPackets []rtcp.Packet) {
	for _, rtcpPacket := range rtcpPackets {
		srPacket, ok := rtcpPacket.(*rtcp.SenderReport)
		if !ok || srPacket.SSRC != w.videoSSRC {
			continue
		}
		if w.videoPacketFirstNTP == 0 && w.videoPacketFirstTs != 0 {
			samples := int64(srPacket.RTPTime) - int64(w.videoPacketFirstTs)
			d := time.Duration(int64(time.Second) * samples / vp8SampleRate)
			w.videoPacketFirstNTP = utils.NtpTime(srPacket.NTPTime).DurationSinceEpoch() - d
		}
		if w.videoSr != nil {
			ntpTimestampDelta := utils.NtpTime(srPacket.NTPTime).DurationSinceEpoch() - utils.NtpTime(w.videoSr.NTPTime).DurationSinceEpoch()
			rtpTimestampDelta := time.Duration(int64(time.Second) * (int64(srPacket.RTPTime) - int64(w.videoSr.RTPTime)) / vp8SampleRate)
			e := ntpTimestampDelta - rtpTimestampDelta
			if e > 200*time.Millisecond || e < -200*time.Millisecond {
				w.ctxLog.Warn("video  ntpTimestampDelta=", ntpTimestampDelta, " rtpTimestampDelta=", rtpTimestampDelta, " e=", e)
			}
		}
		w.videoSr = srPacket
	}
}

func (w *Writer) PushAudioRtcp(rtcpPackets []rtcp.Packet) {
	for _, rtcpPacket := range rtcpPackets {
		srPacket, ok := rtcpPacket.(*rtcp.SenderReport)
		if !ok || srPacket.SSRC != w.audioSSRC {
			continue
		}
		if w.audioPacketFirstNTP == 0 && w.audioPacketFirstTs != 0 {
			samples := int64(srPacket.RTPTime) - int64(w.audioPacketFirstTs)
			d := time.Duration(int64(time.Second) * samples / opusSampleRate)
			w.audioPacketFirstNTP = utils.NtpTime(srPacket.NTPTime).DurationSinceEpoch() - d
		}
		if w.audioSr != nil {
			ntpTimestampDelta := utils.NtpTime(srPacket.NTPTime).DurationSinceEpoch() - utils.NtpTime(w.audioSr.NTPTime).DurationSinceEpoch()
			rtpTimestampDelta := time.Duration(int64(time.Second) * (int64(srPacket.RTPTime) - int64(w.audioSr.RTPTime)) / opusSampleRate)
			e := ntpTimestampDelta - rtpTimestampDelta
			if e > 200*time.Millisecond || e < -200*time.Millisecond {
				w.ctxLog.Warn("audio  ntpTimestampDelta=", ntpTimestampDelta, " rtpTimestampDelta=", rtpTimestampDelta, " e=", e)
			}
		}
		w.audioSr = srPacket
	}
}

func (w *Writer) loop() {
	var audioSample, videoSample *media.Sample
	audioAlive, videoAlive := true, true
	//var lastLogAudioSrFix, lastLogVideoSrFix time.Time
	needKeyframe := true
	for {
		if audioAlive && audioSample == nil {
			audioSample, audioAlive = <-w.audioSampleCh
			if audioAlive {
				for w.audioSr == nil {
					time.Sleep(time.Millisecond * 100)
				}
				audioSr := w.audioSr
				samples := int64(audioSample.PacketTimestamp) - int64(audioSr.RTPTime)
				srFix := time.Duration(int64(time.Second) * samples / opusSampleRate)
				w.audioTimestamp = utils.NtpTime(audioSr.NTPTime).DurationSinceEpoch() - w.audioPacketFirstNTP + srFix + audioSample.Duration
			} else {
				w.audioTimestamp = math.MaxInt64
				for _, audioWriter := range w.audioWriters {
					if err := audioWriter.Close(); err != nil {
						w.ctxLog.WithError(err).Warn("can not close audio block writer")
					}
				}
			}
		}
		if videoAlive && videoSample == nil {
			videoSample, videoAlive = <-w.videoSampleCh
			if videoAlive {
				for w.videoSr == nil {
					time.Sleep(time.Millisecond * 100)
				}
				videoSr := w.videoSr
				samples := int64(videoSample.PacketTimestamp) - int64(videoSr.RTPTime)
				srFix := time.Duration(int64(time.Second) * samples / vp8SampleRate)
				w.videoTimestamp = utils.NtpTime(videoSr.NTPTime).DurationSinceEpoch() - w.videoPacketFirstNTP + srFix + videoSample.Duration
			} else {
				w.videoTimestamp = math.MaxInt64
				for _, videoWriter := range w.videoWriters {
					if err := videoWriter.Close(); err != nil {
						w.ctxLog.WithError(err).Warn("can not close audio block writer")
					}
				}
			}
		}
		if !audioAlive && !videoAlive {
			close(w.VideoKeyframeNeeded)
			return
		}
		if audioSample != nil && w.audioTimestamp <= w.videoTimestamp {
			for _, audioWriter := range w.audioWriters {
				if _, err := audioWriter.Write(true, int64(w.audioTimestamp/time.Millisecond), audioSample.Data); err != nil {
					w.ctxLog.WithError(err).Warn("can not write opus sample")
				}
			}
			audioSample = nil
		} else if videoSample != nil {
			videoKeyframe := videoSample.Data[0]&0x1 == 0
			//if videoKeyframe {
			//	raw := uint(videoSample.Data[6]) | uint(videoSample.Data[7])<<8 | uint(videoSample.Data[8])<<16 | uint(videoSample.Data[9])<<24
			//	width := int(raw & 0x3FFF)
			//	height := int((raw >> 16) & 0x3FFF)
			//	w.ctxLog.Info(fmt.Sprintf("vp8 key frame width=%d height=%d", width, height))
			//}
			if videoKeyframe {
				needKeyframe = false
			} else if videoSample.PrevDroppedPackets > 0 {
				needKeyframe = true
			}
			if needKeyframe {
				w.VideoKeyframeNeeded <- SignalInstance
			}
			for _, videoWriter := range w.videoWriters {
				if _, err := videoWriter.Write(videoKeyframe, int64(w.videoTimestamp/time.Millisecond), videoSample.Data); err != nil {
					w.ctxLog.WithError(err).Warn("can not write vp8 sample")
				}
			}
			videoSample = nil
		}
	}
}
