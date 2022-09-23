package pcap

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v3/pkg/media/samplebuilder"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type KeyframeRequest struct {
	TrackUID uint64
	SSRC     uint32
}

type TrackWriterInfo struct {
	TrackUID     uint64
	SampleRate   uint32
	Depacketizer rtp.Depacketizer
}

type trackWriter struct {
	ctxLog        *log.Entry
	sampleBuilder *samplebuilder.SampleBuilder
	needKeyframe  bool
	info          TrackWriterInfo

	rtpCh    chan packet
	rtcpCh   chan packet
	sampleCh chan packet
}

type Writer struct {
	ctxLog           *log.Entry
	tracks           map[uint64]*trackWriter
	timestamp        time.Time
	KeyframeRequests chan KeyframeRequest
	done             chan struct{}
}

func createPacketWriterChannel(ctxLog *log.Entry, filePath string, headerSize int, wg *sync.WaitGroup) (chan packet, error) {
	ch := make(chan packet)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		defer file.Close()

		writer := bufio.NewWriter(file)
		defer writer.Flush()

		var firstPacketReceived bool
		headerBuf := make([]byte, headerSize)
		for pkt := range ch {
			if !firstPacketReceived {
				ctxLog.Info("first packet received")
				firstPacketReceived = true
			}
			err := writePacket(writer, headerBuf, pkt)
			if err != nil {
				ctxLog.WithError(err).Panic("can not write packet")
			}
		}
		ctxLog.Info("no more packets")
	}()
	return ch, nil
}

func NewWriter(ctxLog *log.Entry, startTime time.Time, dirPath string, tracksInfo []TrackWriterInfo) (*Writer, error) {
	ctxLog = ctxLog.WithField("writerType", "track")

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("can not make all dirs from `%v`, reason = %w", dirPath, err)
	}

	meta := Meta{
		StartTime: startTime,
		Tracks:    make([]MetaTrack, 0, len(tracksInfo)),
	}

	writer := Writer{
		ctxLog:           ctxLog,
		tracks:           make(map[uint64]*trackWriter, len(tracksInfo)),
		timestamp:        time.Now(),
		KeyframeRequests: make(chan KeyframeRequest, 1024),
		done:             make(chan struct{}),
	}
	var wg sync.WaitGroup
	for _, trackInfo := range tracksInfo {
		ctxLog := ctxLog.WithField("trackUID", trackInfo.TrackUID)
		rtpCh, err := createPacketWriterChannel(ctxLog.WithField("writerPart", "rtp"), filepath.Join(dirPath, fmt.Sprintf("%v.rtp", trackInfo.TrackUID)), simpleHeaderSize, &wg)
		if err != nil {
			return nil, err
		}
		rtcpCh, err := createPacketWriterChannel(ctxLog.WithField("writerPart", "rtcp"), filepath.Join(dirPath, fmt.Sprintf("%v.rtcp", trackInfo.TrackUID)), simpleHeaderSize, &wg)
		if err != nil {
			close(rtpCh)
			return nil, err
		}
		sampleCh, err := createPacketWriterChannel(ctxLog.WithField("writerPart", "sample"), filepath.Join(dirPath, fmt.Sprintf("%v.sample", trackInfo.TrackUID)), sampleHeaderSize, &wg)
		if err != nil {
			close(rtpCh)
			close(rtcpCh)
			return nil, err
		}
		metaTrack := MetaTrack{
			Uid:        trackInfo.TrackUID,
			SampleRate: trackInfo.SampleRate,
		}
		var maxLate uint16
		switch trackInfo.Depacketizer.(type) {
		case *codecs.VP8Packet:
			metaTrack.Video = &MetaVideo{
				Width:  360, //todo
				Height: 360, //todo
			}
			maxLate = 1000
		case *codecs.OpusPacket:
			metaTrack.Audio = &MetaAudio{
				Channels: 2, //todo
			}
			maxLate = 10
		default:
			ctxLog.Panicf("not implemented, depacketizer = %v", trackInfo.Depacketizer)
		}
		meta.Tracks = append(meta.Tracks, metaTrack)

		writer.tracks[trackInfo.TrackUID] = &trackWriter{
			ctxLog:        ctxLog,
			info:          trackInfo,
			rtpCh:         rtpCh,
			rtcpCh:        rtcpCh,
			sampleBuilder: samplebuilder.New(maxLate, trackInfo.Depacketizer, trackInfo.SampleRate),
			sampleCh:      sampleCh,
		}
	}
	go func() {
		defer close(writer.done)
		wg.Wait()
		metaData, err := json.MarshalIndent(meta, "", "\t")
		if err != nil {
			ctxLog.WithError(err).Error("can not marshal meta data")
			return
		}
		if err := ioutil.WriteFile(filepath.Join(dirPath, "meta.json"), metaData, 0666); err != nil {
			ctxLog.WithError(err).Error("can not write meta file")
		}
	}()
	return &writer, nil
}

func (w *Writer) PushRTP(trackUID uint64, capturedTime time.Time, rtpPacket *rtp.Packet) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track")
		return
	}

	ctxLog = track.ctxLog
	data, err := rtpPacket.Marshal()
	if err != nil {
		ctxLog.WithError(err).Panic("can not marshal rtp packet")
	}
	track.rtpCh <- &simplePacket{
		capturedTime: capturedTime,
		payload:      data,
	}

	track.sampleBuilder.Push(rtpPacket)

	for {
		sample := track.sampleBuilder.Pop()
		if sample == nil {
			break
		}
		ctxLog.Trace("sample builder produced sample")

		var keyFrame bool
		switch track.info.Depacketizer.(type) {
		case *codecs.VP8Packet:
			keyFrame = sample.Data[0]&0x1 == 0
		case *codecs.OpusPacket:
			keyFrame = true
		default:
			ctxLog.Panicf("not implemented, depacketizer = %v", track.info.Depacketizer)
		}

		if keyFrame {
			track.needKeyframe = false
		} else if sample.PrevDroppedPackets > 0 {
			track.needKeyframe = true
		}

		if track.needKeyframe {
			w.KeyframeRequests <- KeyframeRequest{
				TrackUID: track.info.TrackUID,
				SSRC:     rtpPacket.SSRC,
			}
		} else {
			track.sampleCh <- &samplePacket{
				capturedTime: capturedTime,
				rtpTimestamp: sample.PacketTimestamp,
				payload:      sample.Data,
			}
		}
	}
}

func (w *Writer) PushRTCP(trackUID uint64, capturedTime time.Time, rtcpPackets []rtcp.Packet) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track")
		return
	}

	ctxLog = track.ctxLog
	data, err := rtcp.Marshal(rtcpPackets)
	if err != nil {
		ctxLog.WithError(err).Panic("can not marshal rtcp packets")
	}
	track.rtcpCh <- &simplePacket{
		capturedTime: capturedTime,
		payload:      data,
	}
}

func (w *Writer) NoMoreRTP(trackUID uint64) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track")
		return
	}

	close(track.rtpCh)
	close(track.sampleCh)
}

func (w *Writer) NoMoreRTCP(trackUID uint64) {
	ctxLog := w.ctxLog.WithField("trackUID", trackUID)
	track, ok := w.tracks[trackUID]
	if !ok {
		ctxLog.Error("unknown track")
		return
	}

	close(track.rtcpCh)
}

func (w *Writer) Done() <-chan struct{} {
	return w.done
}

type SimpleWriter struct {
	ctxLog   *log.Entry
	packetCh chan packet
}

func NewSimpleWriter(ctxLog *log.Entry, filePath string) (*SimpleWriter, error) {
	ctxLog = ctxLog.WithField("writerType", "simple")

	if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("can not make all dirs from `%v`, reason = %w", filePath, err)
	}

	if err := os.Remove(filePath); err != nil {
		return nil, fmt.Errorf("can not remove `%v`, reason = %w", filePath, err)
	}

	packetCh, err := createPacketWriterChannel(ctxLog, filePath, simpleHeaderSize, nil)
	if err != nil {
		return nil, fmt.Errorf("can not create packet writer channel, err: %w", err)
	}

	return &SimpleWriter{
		ctxLog:   ctxLog,
		packetCh: packetCh,
	}, nil

}

func (w *SimpleWriter) PushPacket(capturedTime time.Time, packet interface{}) error {
	data, err := json.Marshal(packet)
	if err != nil {
		return fmt.Errorf("can not marshal packet to json, err: %w", err)
	}

	w.packetCh <- &simplePacket{
		capturedTime: capturedTime,
		payload:      data,
	}
	return nil
}

func (w *SimpleWriter) NoMorePacket() {
	close(w.packetCh)
}
