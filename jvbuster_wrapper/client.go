package jvbuster_wrapper

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	jvbuster "github.com/Connect-Club/connectclub-jvbuster-client"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v3"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type ClientState int

const (
	ClientStateStopped ClientState = iota + 1
	ClientStateStarting
	ClientStateStarted
	ClientStateStopping
)

type StreamRemote struct {
	Id               string
	StartTime        time.Time
	PeerConnection   *webrtc.PeerConnection
	AudioTrackFuture *TrackRemoteFuture
	VideoTrackFuture *TrackRemoteFuture
}

type Client struct {
	rawClient       *jvbuster.Client
	localAudioTrack *webrtc.TrackLocalStaticSample
	localVideoTrack *webrtc.TrackLocalStaticSample

	state                     ClientState
	stateMutex                sync.RWMutex
	stopped                   chan struct{}
	streams                   chan StreamRemote
	mainPeerConnectionId      string
	peerConnections           map[ /*PeerConnId*/ string]*webrtc.PeerConnection
	dataChannels              sync.Map
	asdf                      sync.Map
	speaker                   bool
	mainPeerConnectionStateCh chan webrtc.PeerConnectionState
	trackSetters              map[ /*PeerConnId*/ string]map[ /*StreamId*/ string]func(*webrtc.TrackRemote, *webrtc.RTPReceiver)

	log *logrus.Entry
}

func NewClient(
	log *logrus.Entry,
	address string,
	token string,
	localAudioTrack *webrtc.TrackLocalStaticSample,
	localVideoTrack *webrtc.TrackLocalStaticSample,
) *Client {
	client := &Client{
		state:           ClientStateStopped,
		localAudioTrack: localAudioTrack,
		localVideoTrack: localVideoTrack,
		trackSetters:    make(map[ /*PeerConnId*/ string]map[ /*StreamId*/ string]func(*webrtc.TrackRemote, *webrtc.RTPReceiver)),
		log:             log,
	}

	rawClient, err := jvbuster.NewClient(
		address,
		token,
		func(peerConnectionId, msg string) { client.onNewMessageForDataChannel(peerConnectionId, msg) },
		func(sdpOffers map[ /*PeerConnectionId*/ string]jvbuster.SdpOffer, accepted func() error) {
			client.onNewSdpOffers(time.Now(), sdpOffers, accepted)
		},
		func() { client.onExpired() },
		func(endpoints map[string][]string) {
			log.Info("endpoints=", endpoints)
			endpointsToSubscribe := make(map[string]jvbuster.VideoConstraint, 0)
			for _, values := range endpoints {
				for _, value := range values {
					endpointsToSubscribe[value] = jvbuster.VideoConstraint{
						OffVideo: false,
						LowRes:   false,
						LowFps:   false,
					}
				}
			}
			log.Info("endpointsToSubscribe", endpointsToSubscribe)
			client.rawClient.Subscribe(endpointsToSubscribe, jvbuster.VideoConstraint{})
		},
		nil,
		0,
		0,
	)
	if err != nil {
		log.Fatal(err)
	}

	client.rawClient = rawClient
	return client
}

func (client *Client) compareAndSwapState(old, new ClientState) bool {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	if client.state == old {
		client.state = new
		return true
	}
	return false
}

func (client *Client) Start(ctx context.Context, speaker bool) (<-chan StreamRemote, error) {
	if !client.compareAndSwapState(ClientStateStopped, ClientStateStarting) {
		return nil, errors.New("must be stopped before start")
	}
	client.stopped = make(chan struct{})
	client.streams = make(chan StreamRemote)
	client.speaker = speaker
	client.mainPeerConnectionStateCh = make(chan webrtc.PeerConnectionState, 256)
	client.peerConnections = make(map[string]*webrtc.PeerConnection)
	client.dataChannels = sync.Map{}
	client.trackSetters = make(map[ /*PeerConnId*/ string]map[ /*StreamId*/ string]func(*webrtc.TrackRemote, *webrtc.RTPReceiver))

	err := client.rawClient.Start(ctx, speaker, "")
	if err != nil {
		client.internalStop()
		return nil, err
	}
	select {
	case <-ctx.Done():
		client.internalStop()
		return nil, fmt.Errorf("jvbuster start error: %w", ctx.Err())
	case connectionState := <-client.mainPeerConnectionStateCh:
		if connectionState != webrtc.PeerConnectionStateConnected {
			client.internalStop()
			return nil, fmt.Errorf("jvbuster start error: %s", connectionState)
		}
	}

	client.state = ClientStateStarted
	return client.streams, nil
}

func (client *Client) internalStop() {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	client.dataChannels.Range(func(_, value interface{}) bool {
		dataChannel := value.(*webrtc.DataChannel)
		if err := dataChannel.Close(); err != nil {
			client.log.Warn("DataChannel close error", err)
		} else {
			client.log.Info("DataChannel closed")
		}
		return true
	})

	for _, peerConnection := range client.peerConnections {
		err := peerConnection.Close()
		if err != nil {
			client.log.Warn("PeerConnection close error", err)
		} else {
			client.log.Info("PeerConnection closed")
		}
	}
	client.rawClient.Stop()
	client.rawClient.Destroy()

	if client.state != ClientStateStopped {
		client.state = ClientStateStopped
		close(client.stopped)
		close(client.streams)
		for _, trackSetters := range client.trackSetters {
			for _, trackSetter := range trackSetters {
				trackSetter(nil, nil)
			}
		}
	}
}

func (client *Client) Stop() error {
	if !client.compareAndSwapState(ClientStateStarted, ClientStateStopping) {
		return errors.New("must be started before stop")
	}
	client.internalStop()
	return nil
}

func (client *Client) onNewMessageForDataChannel(peerConnectionId, msg string) {
	dataChannel, ok := client.dataChannels.Load(peerConnectionId)
	if !ok {
		return
	}
	err := dataChannel.(*webrtc.DataChannel).SendText(msg)
	if err != nil {
		client.log.Warn("send datachannel msg error", err)
	}
}

var webrtcApi *webrtc.API

func init() {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		logrus.WithError(err).Panic("can not register default codecs")
	}

	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		logrus.WithError(err).Panic("can not register default interceptors")
	}

	s := webrtc.SettingEngine{}
	if err := s.SetAnsweringDTLSRole(webrtc.DTLSRoleServer); err != nil {
		logrus.WithError(err).Panic("SetAnsweringDTLSRole")
	}

	webrtcApi = webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i), webrtc.WithSettingEngine(s))
}

func (c *Client) onNewSdpOffers(capturedTime time.Time, sdpOffers map[ /*PeerConnectionId*/ string]jvbuster.SdpOffer, accepted func() error) {
	c.log.Info("⤵")
	c.log.Infof("len(sdpOffers)=%v", len(sdpOffers))
	defer c.log.Info("⤴")

	answers := make(map[string]string)
	for peerConnectionId, sdpOffer := range sdpOffers {
		if sdpOffer.Text == "" {
			c.log.Infof("peerConnectionId=%v empty sdp", peerConnectionId)
			continue
		}
		peerConnection, ok := c.peerConnections[peerConnectionId]
		if !ok {
			var err error

			c.log.Info("NewPeerConnection")
			peerConnection, err = webrtcApi.NewPeerConnection(webrtc.Configuration{
				PeerIdentity: peerConnectionId,
				SDPSemantics: webrtc.SDPSemanticsUnifiedPlan,
			})
			if err != nil {
				c.log.WithError(err).Fatal("cannot NewPeerConnection")
			} else {
				c.log.Info("NewPeerConnection completed")
			}

			c.peerConnections[peerConnectionId] = peerConnection
			c.trackSetters[peerConnectionId] = make(map[ /*StreamId*/ string]func(*webrtc.TrackRemote, *webrtc.RTPReceiver))
			if sdpOffer.Primary {
				c.mainPeerConnectionId = peerConnectionId
			}
			if sdpOffer.Primary && c.speaker {
				if c.localAudioTrack != nil {
					if _, err := peerConnection.AddTrack(c.localAudioTrack); err != nil {
						c.log.WithError(err).Fatal("cannot add audio track")
					}
				}
				if c.localVideoTrack != nil {
					if _, err := peerConnection.AddTrack(c.localVideoTrack); err != nil {
						c.log.WithError(err).Fatal("cannot add video track")
					}
				}
			}
			peerConnection.OnConnectionStateChange(func(connectionState webrtc.PeerConnectionState) {
				c.onPeerConnConnectionStateChange(peerConnectionId, connectionState)
			})
			peerConnection.OnDataChannel(func(dataChannel *webrtc.DataChannel) {
				c.onPeerConnDataChannel(peerConnectionId, dataChannel)
			})
			peerConnection.OnTrack(func(trackRemote *webrtc.TrackRemote, rtpReceiver *webrtc.RTPReceiver) {
				c.onPeerConnTrack(peerConnectionId, trackRemote, rtpReceiver)
			})

		}
		trackSetters, ok := c.trackSetters[peerConnectionId]
		if !ok {
			c.log.Panicf("unknown peerconnection %v", peerConnectionId)
		}
		for streamId, offerMeta := range sdpOffer.Meta {
			if len(offerMeta.AudioTracks) == 0 || len(offerMeta.VideoTracks) == 0 {
				c.log.Warnf("missing tracks, streamId=%v, len(AudioTracks)=%v, len(VideoTracks)=%v", streamId, len(offerMeta.AudioTracks), len(offerMeta.VideoTracks))
				continue
			}
			audioTrackId := offerMeta.AudioTracks[0]
			_, audioTrackCreated := trackSetters[audioTrackId]
			videoTrackId := offerMeta.VideoTracks[0]
			_, videoTrackCreated := trackSetters[videoTrackId]
			if !audioTrackCreated || !videoTrackCreated {
				audioTrackFuture, audioTrackSetter := newTrackRemoteFuture(webrtc.RTPCodecTypeAudio)
				videoTrackFuture, videoTrackSetter := newTrackRemoteFuture(webrtc.RTPCodecTypeVideo)
				trackSetters[audioTrackId] = audioTrackSetter
				trackSetters[videoTrackId] = videoTrackSetter
				streamRemote := StreamRemote{
					Id:               streamId,
					StartTime:        capturedTime,
					PeerConnection:   peerConnection,
					AudioTrackFuture: audioTrackFuture,
					VideoTrackFuture: videoTrackFuture,
				}
				select {
				case c.streams <- streamRemote:
				case <-time.After(time.Minute):
					c.log.Panic("streams is full")
				}
			}
		}

		c.log.Infof("SetRemoteDescription(%v)", peerConnectionId)
		err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  sdpOffer.Text,
		})
		if err != nil {
			sdp := base64.StdEncoding.EncodeToString([]byte(sdpOffer.Text))
			c.log.WithError(err).WithField("sdp", sdp).Fatal("cannot SetRemoteDescription")
		} else {
			c.log.Info("SetRemoteDescription completed")
		}

		c.log.Info("CreateAnswer")
		answer, err := peerConnection.CreateAnswer(nil)
		if err != nil {
			c.log.WithError(err).Fatal("cannot CreateAnswer")
		} else {
			c.log.Info("CreateAnswer completed")
		}

		answers[peerConnectionId] = answer.SDP
	}

	if len(answers) > 0 {
		//ignore new sdp answers, cause pion does not like answer modifications
		_, err := c.rawClient.ModifyAndSendAnswers(answers)
		if err != nil {
			c.log.WithError(err).Fatal("ProcessAnswers")
		}
		for peerConnectionId, answer := range answers {
			peerConnection, ok := c.peerConnections[peerConnectionId]
			if !ok {
				c.log.Fatalf("unknown peerConnectionId=%v", peerConnectionId)
			}
			err := peerConnection.SetLocalDescription(webrtc.SessionDescription{
				Type: webrtc.SDPTypeAnswer,
				SDP:  answer,
			})
			if err != nil {
				c.log.WithError(err).Fatal("SetLocalDescription")
			}
		}
	}

	if err := accepted(); err != nil {
		c.log.WithError(err).Fatal("cannot accept sdp offers")
	} else {
		c.log.Info("sdp offers accepted")
	}
}

func (client *Client) onExpired() {}

func (client *Client) onPeerConnConnectionStateChange(peerConnectionId string, connectionState webrtc.PeerConnectionState) {
	log := client.log.WithFields(logrus.Fields{
		"peerConnectionId": peerConnectionId,
		"connectionState":  connectionState,
	})
	log.Info("onPeerConnConnectionStateChange")
	if _, ok := client.peerConnections[peerConnectionId]; !ok {
		log.Warn("can not find peerConnection")
		return
	}

	client.stateMutex.RLock()
	state := client.state
	client.stateMutex.RUnlock()

	if peerConnectionId == client.mainPeerConnectionId && state == ClientStateStarting {
		if connectionState == webrtc.PeerConnectionStateConnected ||
			connectionState == webrtc.PeerConnectionStateDisconnected ||
			connectionState == webrtc.PeerConnectionStateFailed ||
			connectionState == webrtc.PeerConnectionStateClosed {
			select {
			case client.mainPeerConnectionStateCh <- connectionState:
			case <-time.After(time.Minute):
				log.Panic("mainPeerConnectionStateCh if full")
			}
		}
	}
}

func (client *Client) onPeerConnDataChannel(peerConnectionId string, dataChannel *webrtc.DataChannel) {
	dataChannel.OnOpen(func() {
		client.dataChannels.Store(peerConnectionId, dataChannel)
	})
	dataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		msgText := string(msg.Data)
		client.log.Info("msgText=", msgText)
		err := client.rawClient.ProcessDataChannelMessage(peerConnectionId, msgText)
		if err != nil {
			client.log.WithError(err).Fatal("cannot ProcessDataChannelMessage")
		}
	})
}

func (client *Client) onPeerConnTrack(peerConnectionId string, trackRemote *webrtc.TrackRemote, rtpReceiver *webrtc.RTPReceiver) {
	log := client.log.WithFields(logrus.Fields{
		"peerConnectionId": peerConnectionId,
		"trackId":          trackRemote.ID(),
		"trackKind":        trackRemote.Kind(),
		"streamId":         trackRemote.StreamID(),
	})
	log.Info("onPeerConnTrack")

	trackSetters, ok := client.trackSetters[peerConnectionId]
	if !ok {
		log.Warn("can not find trackSetters for peerConnectionId")
		return
	}
	trackSetter, ok := trackSetters[trackRemote.ID()]
	if !ok {
		log.Warn("can not find trackSetter")
		return
	}
	trackSetter(trackRemote, rtpReceiver)
}

func (client *Client) Stopped() <-chan struct{} {
	return client.stopped
}
