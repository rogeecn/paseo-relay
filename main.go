package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Keepalive: relay pings every pingPeriod; a peer is considered dead if
	// no pong (or any traffic) arrives within pongWait.
	pongWait   = 60 * time.Second
	pingPeriod = pongWait * 9 / 10
	writeWait  = 10 * time.Second

	// Per-connection outbound queue size. When full, the connection is
	// dropped instead of blocking the relay (backpressure).
	sendBuffer = 1024

	// Max size of a single websocket message forwarded through the relay.
	maxMessageSize = 32 << 20
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// outbound is one message queued for the writer goroutine.
type outbound struct {
	msgType int
	data    []byte
}

type socket struct {
	conn         *websocket.Conn
	version      string
	role         string
	serverId     string
	connectionId string

	send   chan outbound
	closed chan struct{}
	once   sync.Once
}

func newSocket(conn *websocket.Conn, version, role, serverId, connectionId string) *socket {
	return &socket{
		conn:         conn,
		version:      version,
		role:         role,
		serverId:     serverId,
		connectionId: connectionId,
		send:         make(chan outbound, sendBuffer),
		closed:       make(chan struct{}),
	}
}

// enqueue schedules a write. It never touches conn directly, so it is safe to
// call from any goroutine. gorilla/websocket allows only one concurrent
// writer; all writes are serialized through writeLoop.
func (ws *socket) enqueue(msgType int, data []byte) {
	select {
	case ws.send <- outbound{msgType, data}:
	case <-ws.closed:
	default:
		// Peer is not draining fast enough. Drop the connection rather than
		// blocking the relay or growing memory unbounded.
		log.Printf("send buffer overflow, dropping %s(%s) in session %s", ws.role, ws.connectionId, ws.serverId)
		ws.kill()
	}
}

// closeWith sends a close frame (best effort) and terminates the connection.
func (ws *socket) closeWith(code int, text string) {
	ws.enqueue(websocket.CloseMessage, websocket.FormatCloseMessage(code, text))
	go func() {
		time.Sleep(200 * time.Millisecond) // give the writer a moment to flush the close frame
		ws.kill()
	}()
}

func (ws *socket) kill() {
	ws.once.Do(func() {
		close(ws.closed)
		ws.conn.Close()
	})
}

// writeLoop is the ONLY goroutine that calls WriteMessage on the conn.
func (ws *socket) writeLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case m := <-ws.send:
			ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := ws.conn.WriteMessage(m.msgType, m.data); err != nil {
				ws.kill()
				return
			}
		case <-ticker.C:
			ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				ws.kill()
				return
			}
		case <-ws.closed:
			return
		}
	}
}

// readLoop reads until error/timeout, dispatching to onMessage, then
// guarantees conn teardown. It runs in the HTTP handler goroutine.
func (ws *socket) readLoop(onMessage func(msgType int, data []byte)) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("recovered panic on %s(%s) session %s: %v", ws.role, ws.connectionId, ws.serverId, p)
		}
		ws.kill()
	}()

	ws.conn.SetReadLimit(maxMessageSize)
	ws.conn.SetReadDeadline(time.Now().Add(pongWait))
	ws.conn.SetPongHandler(func(string) error {
		ws.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	ws.conn.SetPingHandler(func(appData string) error {
		// WriteControl is documented safe for concurrent use; answering pings
		// here keeps them flowing even when the outbound queue is saturated.
		ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
		err := ws.conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
		ws.conn.SetReadDeadline(time.Now().Add(pongWait))
		return err
	})

	for {
		msgType, data, err := ws.conn.ReadMessage()
		if err != nil {
			return
		}
		onMessage(msgType, data)
	}
}

type session struct {
	mu                sync.Mutex
	controlSockets    []*socket
	serverDataSockets map[string]*socket
	clientSockets     map[string][]*socket
	pendingFrames     map[string][]outbound
}

type relay struct {
	mu       sync.Mutex
	sessions map[string]*session
}

func newRelay() *relay {
	return &relay{sessions: make(map[string]*session)}
}

func (r *relay) getSession(serverId string) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[serverId] == nil {
		r.sessions[serverId] = &session{
			controlSockets:    make([]*socket, 0),
			serverDataSockets: make(map[string]*socket),
			clientSockets:     make(map[string][]*socket),
			pendingFrames:     make(map[string][]outbound),
		}
	}
	return r.sessions[serverId]
}

// cleanupEmpty deletes the session if no sockets remain (fixes the leak:
// removeSession was never called before).
func (r *relay) cleanupEmpty(serverId string, s *session) {
	s.mu.Lock()
	empty := len(s.controlSockets) == 0 && len(s.serverDataSockets) == 0 && len(s.clientSockets) == 0
	s.mu.Unlock()
	if empty {
		r.mu.Lock()
		if r.sessions[serverId] == s {
			delete(r.sessions, serverId)
		}
		r.mu.Unlock()
		log.Printf("Session %s cleaned up", serverId)
	}
}

// syncPayload builds the control-channel "sync" message (list of client ids).
func (s *session) syncPayload() []byte {
	payload, _ := json.Marshal(map[string]interface{}{
		"type":          "sync",
		"connectionIds": s.listClientIds(),
	})
	return payload
}

func (s *session) notifyControls(msg interface{}) {
	data, _ := json.Marshal(msg)
	s.mu.Lock()
	controls := make([]*socket, 0, len(s.controlSockets))
	for _, c := range s.controlSockets {
		select {
		case <-c.closed:
			// Dead socket: prune it instead of writing into the void
			// (official relay closes+forgets controls whose writes fail).
		default:
			controls = append(controls, c)
		}
	}
	s.controlSockets = controls
	s.mu.Unlock()
	for _, c := range controls {
		c.enqueue(websocket.TextMessage, data)
	}
}

// nudgeOrResetControlForConnection mirrors the official relay's self-healing
// for a stalled daemon. A client just connected, so the daemon should open a
// server-data socket for connectionId shortly:
//   - 10s later with no data socket: push a fresh sync to the controls (nudge)
//   - 5s after that, still none: force-close the controls (1011) so the daemon
//     reconnects and re-syncs. Without this, a half-dead control channel
//     leaves clients spinning forever.
func (r *relay) nudgeOrResetControlForConnection(serverId string, s *session, connectionId string) {
	time.AfterFunc(10*time.Second, func() {
		s.mu.Lock()
		hasData := s.serverDataSockets[connectionId] != nil
		hasClients := len(s.clientSockets[connectionId]) > 0
		controls := append([]*socket(nil), s.controlSockets...)
		s.mu.Unlock()
		if hasData || !hasClients {
			return
		}
		log.Printf("v2: no data socket for %s in session %s 10s after client connect, nudging controls", connectionId, serverId)
		payload := s.syncPayload()
		for _, c := range controls {
			c.enqueue(websocket.TextMessage, payload)
		}

		time.AfterFunc(5*time.Second, func() {
			s.mu.Lock()
			hasData := s.serverDataSockets[connectionId] != nil
			hasClients := len(s.clientSockets[connectionId]) > 0
			controls := append([]*socket(nil), s.controlSockets...)
			s.mu.Unlock()
			if hasData || !hasClients || len(controls) == 0 {
				return
			}
			log.Printf("v2: control unresponsive for %s in session %s, forcing control reconnect", connectionId, serverId)
			for _, c := range controls {
				c.closeWith(1011, "Control unresponsive")
			}
		})
	})
}

func (s *session) listClientIds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0)
	for id, socks := range s.clientSockets {
		if len(socks) > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *session) bufferFrame(connId string, m outbound) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := s.pendingFrames[connId]
	frames = append(frames, m)
	if len(frames) > 200 {
		frames = frames[len(frames)-200:]
	}
	s.pendingFrames[connId] = frames
}

// flushFrames drains frames buffered while the server data socket was away.
// It must be called BEFORE publishing ws into serverDataSockets, so buffered
// frames are queued ahead of any live traffic (FIFO through the same writer).
func (s *session) flushFrames(connId string, ws *socket) {
	s.mu.Lock()
	frames := s.pendingFrames[connId]
	delete(s.pendingFrames, connId)
	s.mu.Unlock()

	for _, f := range frames {
		ws.enqueue(f.msgType, f.data)
	}
}

func (r *relay) handleV1(ws *socket) {
	s := r.getSession(ws.serverId)
	go ws.writeLoop()

	ws.readLoop(func(msgType int, data []byte) {
		s.mu.Lock()
		var targets []*socket
		if ws.role == "client" {
			// client -> server side
			targets = append(targets, s.controlSockets...)
			for _, ds := range s.serverDataSockets {
				targets = append(targets, ds)
			}
		} else {
			// server -> all clients
			for _, socks := range s.clientSockets {
				targets = append(targets, socks...)
			}
		}
		s.mu.Unlock()
		for _, t := range targets {
			t.enqueue(msgType, data)
		}
	})
}

func (r *relay) handleV2Control(ws *socket) {
	s := r.getSession(ws.serverId)
	s.mu.Lock()
	// Identity preemption (matches official relay): a new control connection
	// for the same serverId kicks the old one, so a zombie socket can never
	// split the control route.
	for _, old := range s.controlSockets {
		log.Printf("v2: preempting stale control socket in session %s (1008)", ws.serverId)
		old.closeWith(1008, "Replaced by new connection")
	}
	s.controlSockets = []*socket{ws}
	s.mu.Unlock()

	go ws.writeLoop()

	ws.enqueue(websocket.TextMessage, s.syncPayload())

	ws.readLoop(func(msgType int, data []byte) {
		var msg map[string]interface{}
		if json.Unmarshal(data, &msg) == nil {
			if msg["type"] == "ping" {
				pong, _ := json.Marshal(map[string]interface{}{"type": "pong", "ts": time.Now().UnixMilli()})
				ws.enqueue(websocket.TextMessage, pong)
			}
		}
	})

	s.mu.Lock()
	s.controlSockets = removeSocket(s.controlSockets, ws)
	s.mu.Unlock()
	log.Printf("v2:server(control) disconnected from session %s", ws.serverId)
	r.cleanupEmpty(ws.serverId, s)
}

func (r *relay) handleV2ServerData(ws *socket) {
	s := r.getSession(ws.serverId)

	go ws.writeLoop()

	// Flush buffered frames first, then publish: guarantees ordered delivery.
	s.flushFrames(ws.connectionId, ws)
	s.mu.Lock()
	// Identity preemption (matches official relay): a new data socket for the
	// same connectionId kicks the old one.
	if old, ok := s.serverDataSockets[ws.connectionId]; ok && old != ws {
		log.Printf("v2: preempting stale data socket for %s in session %s (1008)", ws.connectionId, ws.serverId)
		old.closeWith(1008, "Replaced by new connection")
	}
	s.serverDataSockets[ws.connectionId] = ws
	s.mu.Unlock()

	ws.readLoop(func(msgType int, data []byte) {
		s.mu.Lock()
		clients := append([]*socket(nil), s.clientSockets[ws.connectionId]...)
		s.mu.Unlock()
		for _, c := range clients {
			c.enqueue(msgType, data)
		}
	})

	s.mu.Lock()
	// Only announce "server disconnected" if this socket was still the
	// registered one — a preempted socket must not disturb the new one's
	// clients.
	if cur, ok := s.serverDataSockets[ws.connectionId]; ok && cur == ws {
		delete(s.serverDataSockets, ws.connectionId)
		for _, c := range s.clientSockets[ws.connectionId] {
			c.closeWith(1012, "Server disconnected")
		}
	}
	s.mu.Unlock()
	log.Printf("v2:server(data:%s) disconnected from session %s", ws.connectionId, ws.serverId)
	r.cleanupEmpty(ws.serverId, s)
}

func (r *relay) handleV2Client(ws *socket) {
	// If client doesn't provide a connectionId, generate one
	if ws.connectionId == "" {
		u := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
		ws.connectionId = "conn_" + u
		log.Printf("Generated connectionId %s for client in session %s", ws.connectionId, ws.serverId)
	}

	s := r.getSession(ws.serverId)
	s.mu.Lock()
	s.clientSockets[ws.connectionId] = append(s.clientSockets[ws.connectionId], ws)
	s.mu.Unlock()

	go ws.writeLoop()

	// Notify daemon that a client connected, and arm the control watchdog:
	// the daemon should open a data socket for this connectionId soon.
	s.notifyControls(map[string]interface{}{
		"type":         "connected",
		"connectionId": ws.connectionId,
	})
	r.nudgeOrResetControlForConnection(ws.serverId, s, ws.connectionId)

	ws.readLoop(func(msgType int, data []byte) {
		// Try to extract connectionId from e2ee handshake message.
		// Cheap guard: only parse small text frames that mention the key.
		if msgType == websocket.TextMessage && len(data) <= 4096 && bytes.Contains(data, []byte("connectionId")) {
			var msg map[string]interface{}
			if json.Unmarshal(data, &msg) == nil {
				if cid, ok := msg["connectionId"].(string); ok && cid != "" && ws.connectionId != cid {
					log.Printf("Client connectionId updated from %s to %s", ws.connectionId, cid)
					s.mu.Lock()
					s.clientSockets[ws.connectionId] = removeSocket(s.clientSockets[ws.connectionId], ws)
					ws.connectionId = cid
					s.clientSockets[ws.connectionId] = append(s.clientSockets[ws.connectionId], ws)
					s.mu.Unlock()
					s.notifyControls(map[string]interface{}{
						"type":         "connected",
						"connectionId": ws.connectionId,
					})
					r.nudgeOrResetControlForConnection(ws.serverId, s, ws.connectionId)
				}
			}
		}

		s.mu.Lock()
		serverWs := s.serverDataSockets[ws.connectionId]
		s.mu.Unlock()
		if serverWs == nil {
			s.bufferFrame(ws.connectionId, outbound{msgType, data})
			return
		}
		serverWs.enqueue(msgType, data)
	})

	// Cleanup on disconnect.
	s.mu.Lock()
	remaining := removeSocket(s.clientSockets[ws.connectionId], ws)
	s.clientSockets[ws.connectionId] = remaining
	last := len(remaining) == 0
	var ds *socket
	if last {
		delete(s.clientSockets, ws.connectionId)
		delete(s.pendingFrames, ws.connectionId)
		ds = s.serverDataSockets[ws.connectionId]
		if ds != nil {
			delete(s.serverDataSockets, ws.connectionId)
		}
	}
	s.mu.Unlock()

	if last {
		if ds != nil {
			ds.closeWith(1001, "Client disconnected")
		}
		s.notifyControls(map[string]interface{}{
			"type":         "disconnected",
			"connectionId": ws.connectionId,
		})
	}
	log.Printf("v2:client(%s) disconnected from session %s", ws.connectionId, ws.serverId)
	r.cleanupEmpty(ws.serverId, s)
}

func removeSocket(list []*socket, target *socket) []*socket {
	result := make([]*socket, 0, len(list))
	for _, s := range list {
		if s != target {
			result = append(result, s)
		}
	}
	return result
}

func (r *relay) handleWS(w http.ResponseWriter, req *http.Request) {
	role := req.URL.Query().Get("role")
	serverId := req.URL.Query().Get("serverId")
	connectionId := strings.TrimSpace(req.URL.Query().Get("connectionId"))
	version := req.URL.Query().Get("v")
	if version == "" {
		version = "2"
	}

	log.Printf("WS request: role=%s serverId=%s connectionId=%s v=%s from=%s",
		role, serverId, connectionId, version, req.RemoteAddr)

	if role != "server" && role != "client" {
		http.Error(w, "Missing or invalid role", http.StatusBadRequest)
		return
	}
	if serverId == "" {
		http.Error(w, "Missing serverId", http.StatusBadRequest)
		return
	}
	if version != "1" && version != "2" {
		http.Error(w, "Invalid version (expected 1 or 2)", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}

	log.Printf("WS upgraded: role=%s serverId=%s connectionId=%s v=%s from=%s",
		role, serverId, connectionId, version, conn.RemoteAddr())

	ws := newSocket(conn, version, role, serverId, connectionId)

	switch {
	case version == "1":
		r.handleV1(ws)
	case role == "server" && connectionId == "":
		r.handleV2Control(ws)
	case role == "server" && connectionId != "":
		r.handleV2ServerData(ws)
	case role == "client":
		r.handleV2Client(ws)
	}
}

func main() {
	defaultPort := os.Getenv("PORT")
	if defaultPort == "" {
		defaultPort = "8443"
	}
	port := flag.String("port", defaultPort, "listen port (env: PORT)")
	flag.Parse()

	r := newRelay()

	http.HandleFunc("/ws", r.handleWS)
	http.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		count := len(r.sessions)
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"sessions": count,
		})
	})

	log.Printf("Paseo relay listening on port %s", *port)
	log.Printf("WebSocket endpoint: ws://localhost:%s/ws", *port)
	log.Printf("Health check: http://localhost:%s/health", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatal(err)
	}
}
