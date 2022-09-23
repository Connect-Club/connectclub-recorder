package pcap

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	sampleHeaderSize = 16
	simpleHeaderSize = 12
)

type packet interface {
	getHeader(buf []byte) []byte
	setHeader(buf []byte) uint32
	getPayload() []byte
	setPayload(payload []byte)
}

type samplePacket struct {
	capturedTime time.Time
	rtpTimestamp uint32
	payload      []byte
}

func newSamplePacket() packet {
	return &samplePacket{}
}

func (pkt *samplePacket) getHeader(buf []byte) []byte {
	sec := pkt.capturedTime.Unix()
	nsec := pkt.capturedTime.Nanosecond()

	binary.LittleEndian.PutUint32(buf[0:4], uint32(sec))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(nsec))
	binary.LittleEndian.PutUint32(buf[8:12], pkt.rtpTimestamp)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(pkt.payload)))

	return buf[:sampleHeaderSize]
}

func (pkt *samplePacket) setHeader(buf []byte) uint32 {
	sec := binary.LittleEndian.Uint32(buf[0:4])
	nsec := binary.LittleEndian.Uint32(buf[4:8])
	pkt.capturedTime = time.Unix(int64(sec), int64(nsec))

	pkt.rtpTimestamp = binary.LittleEndian.Uint32(buf[8:12])
	return binary.LittleEndian.Uint32(buf[12:16])
}

func (pkt *samplePacket) getPayload() []byte {
	return pkt.payload
}

func (pkt *samplePacket) setPayload(payload []byte) {
	pkt.payload = payload
}

type simplePacket struct {
	capturedTime time.Time
	payload      []byte
}

func newSimplePacket() packet {
	return &simplePacket{}
}

func (pkt *simplePacket) getHeader(buf []byte) []byte {
	secs := pkt.capturedTime.Unix()
	usecs := pkt.capturedTime.Nanosecond()

	binary.LittleEndian.PutUint32(buf[0:4], uint32(secs))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(usecs))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(pkt.payload)))

	return buf[:simpleHeaderSize]
}

func (pkt *simplePacket) setHeader(buf []byte) uint32 {
	sec := binary.LittleEndian.Uint32(buf[0:4])
	nsec := binary.LittleEndian.Uint32(buf[4:8])
	pkt.capturedTime = time.Unix(int64(sec), int64(nsec))

	return binary.LittleEndian.Uint32(buf[8:12])
}

func (pkt *simplePacket) getPayload() []byte {
	return pkt.payload
}

func (pkt *simplePacket) setPayload(payload []byte) {
	pkt.payload = payload
}

func writePacket(w io.Writer, headerBuf []byte, pkt packet) error {
	if _, err := w.Write(pkt.getHeader(headerBuf)); err != nil {
		return fmt.Errorf("error writing packet header: %w", err)
	}
	if _, err := w.Write(pkt.getPayload()); err != nil {
		return fmt.Errorf("error writing packet payload: %w", err)
	}
	return nil
}

func readPacket(r io.Reader, headerBuf []byte, pkt packet) error {
	if n, err := io.ReadFull(r, headerBuf); n != len(headerBuf) || err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("can not read packet header, n=%v, err=%w", n, err)
	}
	payloadSize := pkt.setHeader(headerBuf)
	payload := make([]byte, payloadSize)
	if n, err := io.ReadFull(r, payload); n != len(payload) || err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("can not read packet payload, n=%v, err=%w", n, err)
	}
	pkt.setPayload(payload)
	return nil
}

type MetaAudio struct {
	Channels uint64 `json:"channels"`
}

type MetaVideo struct {
	Width  uint64 `json:"width"`
	Height uint64 `json:"height"`
}

type MetaTrack struct {
	Uid        uint64     `json:"uid"`
	SampleRate uint32     `json:"sampleRate"`
	Audio      *MetaAudio `json:"audio,omitempty"`
	Video      *MetaVideo `json:"video,omitempty"`
}

type Meta struct {
	StartTime time.Time   `json:"startTime"`
	Tracks    []MetaTrack `json:"tracks"`
}
