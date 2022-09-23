package jvbuster_wrapper

import (
	"github.com/pion/webrtc/v3"
	"sync"
)

type TrackRemoteFuture struct {
	kind        webrtc.RTPCodecType
	trackRemote *webrtc.TrackRemote
	rtpReceiver *webrtc.RTPReceiver
	done        bool
	mu          sync.Mutex
	callbacks   []func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
}

func newTrackRemoteFuture(kind webrtc.RTPCodecType) (future *TrackRemoteFuture, setter func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	future = &TrackRemoteFuture{
		kind:      kind,
		callbacks: make([]func(*webrtc.TrackRemote, *webrtc.RTPReceiver), 0, 1),
	}
	setter = func(trackRemote *webrtc.TrackRemote, rtpReceiver *webrtc.RTPReceiver) {
		future.mu.Lock()
		defer future.mu.Unlock()

		if future.done {
			return
		}
		future.trackRemote = trackRemote
		future.rtpReceiver = rtpReceiver
		future.done = true
		for _, callback := range future.callbacks {
			callback(trackRemote, rtpReceiver)
		}
	}
	return
}

func (f *TrackRemoteFuture) OnDone(callback func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		callback(f.trackRemote, f.rtpReceiver)
	} else {
		f.callbacks = append(f.callbacks, callback)
	}
}

func (f *TrackRemoteFuture) Kind() webrtc.RTPCodecType {
	return f.kind
}
