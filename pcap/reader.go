package pcap

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3/pkg/media"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type trackReader struct {
	ctxLog *log.Entry

	rtpCh    chan packet
	rtcpCh   chan packet
	sampleCh chan packet
}

type Reader struct {
	ctxLog *log.Entry
	tracks map[uint64]*trackReader
	Meta   Meta
}

func createPacketReaderChannel(ctxLog *log.Entry, filePath string, headerSize int, newPacketFn func() packet) (chan packet, error) {
	ch := make(chan packet)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	go func() {
		defer file.Close()
		defer close(ch)

		reader := bufio.NewReader(file)

		headerBuf := make([]byte, headerSize)

		for {
			pkt := newPacketFn()
			if err := readPacket(reader, headerBuf, pkt); err != nil {
				if err != io.EOF {
					ctxLog.WithError(err).Warn("can not read packet")
				}
				return
			}
			ch <- pkt
		}
	}()
	return ch, nil
}

func NewReader(ctxLog *log.Entry, dirPath string) (*Reader, error) {
	metaFile, err := os.Open(filepath.Join(dirPath, "meta.json"))
	if err != nil {
		return nil, fmt.Errorf("can not open meta file, %w", err)
	}
	defer metaFile.Close()

	metaData, err := ioutil.ReadAll(metaFile)
	if err != nil {
		return nil, fmt.Errorf("can not read data from meta file, %w", err)
	}

	var meta Meta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("can not unmarshal data from meta file, %w", err)
	}

	reader := Reader{
		ctxLog: ctxLog,
		tracks: make(map[uint64]*trackReader, len(meta.Tracks)),
		Meta:   meta,
	}
	for _, trackInfo := range meta.Tracks {
		ctxLog := ctxLog.WithField("trackUID", trackInfo.Uid)
		rtpCh, err := createPacketReaderChannel(ctxLog, filepath.Join(dirPath, fmt.Sprintf("%v.rtp", trackInfo.Uid)), simpleHeaderSize, newSimplePacket)
		if err != nil {
			return nil, err
		}
		rtcpCh, err := createPacketReaderChannel(ctxLog, filepath.Join(dirPath, fmt.Sprintf("%v.rtcp", trackInfo.Uid)), simpleHeaderSize, newSimplePacket)
		if err != nil {
			//todo: close rtpCh
			return nil, err
		}
		sampleCh, err := createPacketReaderChannel(ctxLog, filepath.Join(dirPath, fmt.Sprintf("%v.sample", trackInfo.Uid)), sampleHeaderSize, newSamplePacket)
		if err != nil {
			//todo: close rtpCh
			//todo: close rtcpCh
			return nil, err
		}
		reader.tracks[trackInfo.Uid] = &trackReader{
			ctxLog:   ctxLog,
			rtpCh:    rtpCh,
			rtcpCh:   rtcpCh,
			sampleCh: sampleCh,
		}
	}
	return &reader, nil
}

func (r *Reader) PopRTP(trackUID uint64) (rtpPacket *rtp.Packet, capturedTime time.Time) {
	ctxLog := r.ctxLog.WithField("trackUID", trackUID)
	track, ok := r.tracks[trackUID]
	if !ok {
		ctxLog.Panic("unknown track")
	}

	ctxLog = track.ctxLog

	pkt, ok := <-track.sampleCh
	if !ok {
		return nil, time.Time{}
	}
	simplePkt, ok := pkt.(*simplePacket)
	if !ok {
		ctxLog.Panic("not a simple packet")
	}
	capturedTime = simplePkt.capturedTime
	rtpPacket = &rtp.Packet{}
	err := rtpPacket.Unmarshal(simplePkt.payload)
	if err != nil {
		ctxLog.WithError(err).Panic("nat a rtp packet")
	}
	return
}

func (r *Reader) PopRTCP(trackUID uint64) (rtcpPackets []rtcp.Packet, capturedTime time.Time) {
	ctxLog := r.ctxLog.WithField("trackUID", trackUID)
	track, ok := r.tracks[trackUID]
	if !ok {
		ctxLog.Panic("unknown track")
	}

	ctxLog = track.ctxLog

	pkt, ok := <-track.rtcpCh
	if !ok {
		return nil, time.Time{}
	}
	simplePkt, ok := pkt.(*simplePacket)
	if !ok {
		ctxLog.Panic("not a simple packet")
	}
	capturedTime = simplePkt.capturedTime
	rtcpPackets, err := rtcp.Unmarshal(simplePkt.payload)
	if err != nil {
		ctxLog.WithError(err).Panic("not rtcp packets")
	}
	return
}

func (r *Reader) PopSample(trackUID uint64) (mediaSample *media.Sample, capturedTime time.Time) {
	ctxLog := r.ctxLog.WithField("trackUID", trackUID)
	track, ok := r.tracks[trackUID]
	if !ok {
		ctxLog.Panic("unknown track")
	}

	ctxLog = track.ctxLog

	pkt, ok := <-track.sampleCh
	if !ok {
		return nil, time.Time{}
	}
	samplePkt, ok := pkt.(*samplePacket)
	if !ok {
		ctxLog.Panic("not a sample packet")
	}
	return &media.Sample{
		PacketTimestamp: samplePkt.rtpTimestamp,
		Data:            samplePkt.payload,
	}, samplePkt.capturedTime
}

type SimpleReader struct {
	ctxLog *log.Entry
	packetCh chan packet
}

func NewSimpleReader(ctxLog *log.Entry, filePath string) (*SimpleReader, error) {
	ctxLog = ctxLog.WithField("readerType", "simple")
	packetCh, err := createPacketReaderChannel(ctxLog, filePath, simpleHeaderSize, newSimplePacket)
	if err != nil {
		return nil, fmt.Errorf("can not create packet reader channel, err: %w", err)
	}

	return &SimpleReader{
		ctxLog: ctxLog,
		packetCh: packetCh,
	}, nil
}

func (r *SimpleReader) PopPacket(packet interface{}) (capturedTime time.Time) {
	pkt, ok := <-r.packetCh
	if !ok {
		return time.Time{}
	}

	simplePkt, ok := pkt.(*simplePacket)
	if !ok {
		r.ctxLog.Panic("not a simple packet")
	}
	if err := json.Unmarshal(simplePkt.payload, packet); err != nil {
		r.ctxLog.WithError(err).Panic("can not unmarshal packet")
	}
	return simplePkt.capturedTime
}