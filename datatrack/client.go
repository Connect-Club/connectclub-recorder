package datatrack

import (
	"connectclub-recorder/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strings"
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

const (
	// Time allowed to write a message to the peer.
	writeWait = 5 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 10 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 8) / 10
)

type Client struct {
	state        ClientState
	stateMutex   sync.Mutex
	stopped      chan struct{}
	conn         *websocket.Conn
	address      string
	accessToken  string
	roomId       string
	roomPassword string
}

func NewClient(address, accessToken, roomId, roomPassword string) *Client {
	return &Client{
		state:        ClientStateStopped,
		address:      address,
		accessToken:  accessToken,
		roomId:       roomId,
		roomPassword: roomPassword,
	}
}

func (c *Client) compareAndSwapState(old, new ClientState) bool {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()

	if c.state == old {
		c.state = new
		return true
	}
	return false
}

func (c *Client) Start(ctx context.Context) (string, <-chan *CapturedMessage, time.Time, error) {
	if !c.compareAndSwapState(ClientStateStopped, ClientStateStarting) {
		return "", nil, time.Time{}, errors.New("must be stopped before start")
	}
	c.stopped = make(chan struct{})

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.address, nil)
	if err != nil {
		c.internalStop()
		return "", nil, time.Time{}, fmt.Errorf("can not dial to websocket by address=%v, err=%w", c.address, err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(payload string) error {
		log.Infof("pong(%v) received", payload)
		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			log.WithError(err).Error("cannot set read deadline")
		}
		return nil
	})

	c.conn = conn

	err = conn.WriteJSON(utils.JsObject{
		"type": "register",
		"payload": utils.JsObject{
			"sid":         c.roomId,
			"password":    c.roomPassword,
			"accessToken": c.accessToken,
			"version":     3,
		},
	})
	if err != nil {
		c.internalStop()
		return "", nil, time.Time{}, fmt.Errorf("can not send register message, err=%w", err)
	}

	registered := make(chan error, 1)

	var userId string
	go func() {
		defer close(registered)

		var firstMessage Message
		var registerPayload map[string]interface{}
		if err := conn.ReadJSON(&firstMessage); err != nil {
			registered <- fmt.Errorf("can not read json from websocket connection, err: %w", err)
		} else if firstMessage.Type == "access_token_invalid" && firstMessage.Description == "Service client cannot create room" {
			registered <- errors.New("room-not-found")
		} else if firstMessage.Type != "register" {
			registered <- fmt.Errorf("expected 'register' message type, got '%v'", firstMessage.Type)
		} else if err := json.Unmarshal(firstMessage.Payload, &registerPayload); err != nil {
			registered <- fmt.Errorf("can not unmarshal payload for 'register' message, err: %w", err)
		} else {
			userId = registerPayload["id"].(string)
		}
	}()

	select {
	case <-ctx.Done():
		c.internalStop()
		return "", nil, time.Time{}, fmt.Errorf("datatrack client start error: %w", ctx.Err())
	case err := <-registered:
		if err == nil {
			startTime := time.Now()
			c.state = ClientStateStarted
			messageCh := make(chan *CapturedMessage, 1024)
			go c.loop(messageCh)
			go c.ping()
			return userId, messageCh, startTime, nil
		} else {
			c.internalStop()
			return "", nil, time.Time{}, fmt.Errorf("datatrack client register error: %w", err)
		}
	}
}

func (c *Client) loop(messageCh chan<- *CapturedMessage) {
	defer c.internalStop()

	conn := c.conn
	if conn == nil {
		log.Error("conn is nil")
		return
	}
	for {
		if c.state == ClientStateStopped {
			return
		}
		var msg Message
		err := conn.ReadJSON(&msg)
		capturedTime := time.Now()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") && !strings.Contains(err.Error(), "close 1005") {
				log.WithError(err).Error("can not read json from websocket connection")
			} else {
				log.WithError(err).Info("websocket closed")
			}
			close(messageCh)
			break
		}
		capturedMessage := &CapturedMessage{
			CapturedTime: capturedTime,
			RawMessage:   &msg,
		}
		select {
		case messageCh <- capturedMessage:
		case <-time.After(time.Minute):
			log.Panic("messageCh is full")
		}
	}
}

func (c *Client) ping() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	conn := c.conn
	if conn == nil {
		log.Error("conn is nil")
		return
	}
	for {
		select {
		case <-ticker.C:
			if c.state == ClientStateStopped {
				return
			}
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait)); err != nil {
				log.WithError(err).Warn("cannot write ping message")
				return
			}
		case <-c.stopped:
			return
		}
	}
}

func (c *Client) internalStop() {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			log.WithError(err).Warn("close websocket error")
		}
		c.conn = nil
	}

	if c.state != ClientStateStopped {
		c.state = ClientStateStopped
		close(c.stopped)
	}
}

func (c *Client) Stop() error {
	if !c.compareAndSwapState(ClientStateStarted, ClientStateStopping) {
		return errors.New("must be started before stop")
	}

	c.internalStop()
	return nil
}

func (c *Client) Stopped() <-chan struct{} {
	return c.stopped
}
