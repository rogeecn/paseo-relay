package main

import (
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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type session struct {
	mu                sync.Mutex
	controlSockets    []*socket
	serverDataSockets map[string]*socket
	clientSockets     map[string][]*socket
	pendingFrames     map[string][]json.RawMessage
}

type socket struct {
	conn         *websocket.Conn
	version      string
	role         string
	serverId     string
	connectionId string
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
			pendingFrames:     make(map[string][]json.RawMessage),
		}
	}
	return r.sessions[serverId]
}

func (r *relay) removeSession(serverId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, serverId)
}

func (s *session) notifyControls(msg interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _ := json.Marshal(msg)
	for _, ws := range s.controlSockets {
		if err := ws.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			ws.conn.Close()
		}
	}
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

func (s *session) bufferFrame(connId string, data json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := s.pendingFrames[connId]
	frames = append(frames, data)
	if len(frames) > 200 {
		frames = frames[len(frames)-200:]
	}
	s.pendingFrames[connId] = frames
}

func (s *session) flushFrames(connId string, ws *socket) {
	s.mu.Lock()
	frames := s.pendingFrames[connId]
	s.pendingFrames[connId] = nil
	delete(s.pendingFrames, connId)
	s.mu.Unlock()

	for _, f := range frames {
		if err := ws.conn.WriteMessage(websocket.TextMessage, f); err != nil {
			s.bufferFrame(connId, f)
			break
		}
	}
}

func (s *session) cleanupEmpty(serverId string) {
	s.mu.Lock()
	empty := len(s.controlSockets) == 0 && len(s.serverDataSockets) == 0 && len(s.clientSockets) == 0
	s.mu.Unlock()
	if empty {
		log.Printf("Session %s cleaned up", serverId)
	}
}

func (r *relay) handleV1(ws *socket) {
	s := r.getSession(ws.serverId)

	go func() {
		for {
			_, data, err := ws.conn.ReadMessage()
			if err != nil {
				log.Printf("v1:%s disconnected from session %s", ws.role, ws.serverId)
				return
			}
			s.mu.Lock()
			targetRole := "client"
			if ws.role == "client" {
				targetRole = "server"
			}
			if targetRole == "client" {
				for _, socks := range s.clientSockets {
					for _, c := range socks {
						c.conn.WriteMessage(websocket.TextMessage, data)
					}
				}
			} else {
				for _, ctrl := range s.controlSockets {
					ctrl.conn.WriteMessage(websocket.TextMessage, data)
				}
				for _, ds := range s.serverDataSockets {
					ds.conn.WriteMessage(websocket.TextMessage, data)
				}
			}
			s.mu.Unlock()
		}
	}()
}

func (r *relay) handleV2Control(ws *socket) {
	s := r.getSession(ws.serverId)
	s.mu.Lock()
	s.controlSockets = append(s.controlSockets, ws)
	s.mu.Unlock()

	syncMsg, _ := json.Marshal(map[string]interface{}{
		"type":          "sync",
		"connectionIds": s.listClientIds(),
	})
	ws.conn.WriteMessage(websocket.TextMessage, syncMsg)

	go func() {
		for {
			_, data, err := ws.conn.ReadMessage()
			if err != nil {
				s.mu.Lock()
				s.controlSockets = removeSocket(s.controlSockets, ws)
				s.mu.Unlock()
				log.Printf("v2:server(control) disconnected from session %s", ws.serverId)
				s.cleanupEmpty(ws.serverId)
				return
			}
			var msg map[string]interface{}
			if json.Unmarshal(data, &msg) == nil {
				if msg["type"] == "ping" {
					pong, _ := json.Marshal(map[string]interface{}{"type": "pong", "ts": time.Now().UnixMilli()})
					ws.conn.WriteMessage(websocket.TextMessage, pong)
				}
			}
		}
	}()
}

func (r *relay) handleV2ServerData(ws *socket) {
	s := r.getSession(ws.serverId)
	s.mu.Lock()
	s.serverDataSockets[ws.connectionId] = ws
	s.mu.Unlock()

	r.flushFramesTo(ws)

	go func() {
		for {
			_, data, err := ws.conn.ReadMessage()
			if err != nil {
				s.mu.Lock()
				delete(s.serverDataSockets, ws.connectionId)
				for _, c := range s.clientSockets[ws.connectionId] {
					c.conn.WriteMessage(websocket.CloseMessage,
						websocket.FormatCloseMessage(1012, "Server disconnected"))
				}
				delete(s.clientSockets, ws.connectionId)
				s.mu.Unlock()
				log.Printf("v2:server(data:%s) disconnected from session %s", ws.connectionId, ws.serverId)
				s.cleanupEmpty(ws.serverId)
				return
			}
			s.mu.Lock()
			clients := s.clientSockets[ws.connectionId]
			s.mu.Unlock()
			for _, c := range clients {
				c.conn.WriteMessage(websocket.TextMessage, data)
			}
		}
	}()
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

	// Notify daemon that a client connected
	s.notifyControls(map[string]interface{}{
		"type":          "connected",
		"connectionId":  ws.connectionId,
	})

	go func() {
		for {
			_, data, err := ws.conn.ReadMessage()
			if err != nil {
				s.mu.Lock()
				s.clientSockets[ws.connectionId] = removeSocket(s.clientSockets[ws.connectionId], ws)
				if len(s.clientSockets[ws.connectionId]) == 0 {
					delete(s.clientSockets, ws.connectionId)
					delete(s.pendingFrames, ws.connectionId)
					if ds, ok := s.serverDataSockets[ws.connectionId]; ok {
						ds.conn.WriteMessage(websocket.CloseMessage,
							websocket.FormatCloseMessage(1001, "Client disconnected"))
						delete(s.serverDataSockets, ws.connectionId)
					}
					s.mu.Unlock()
					s.notifyControls(map[string]interface{}{
						"type":         "disconnected",
						"connectionId": ws.connectionId,
					})
				} else {
					s.mu.Unlock()
				}
				log.Printf("v2:client(%s) disconnected from session %s", ws.connectionId, ws.serverId)
				s.cleanupEmpty(ws.serverId)
				return
			}

			// Try to extract connectionId from e2ee handshake message
			var msg map[string]interface{}
			if json.Unmarshal(data, &msg) == nil {
				if cid, ok := msg["connectionId"].(string); ok && cid != "" && ws.connectionId != cid {
					log.Printf("Client connectionId updated from %s to %s", ws.connectionId, cid)
					s.mu.Lock()
					// Move socket from old connectionId to new one
					s.clientSockets[ws.connectionId] = removeSocket(s.clientSockets[ws.connectionId], ws)
					ws.connectionId = cid
					s.clientSockets[ws.connectionId] = append(s.clientSockets[ws.connectionId], ws)
					s.mu.Unlock()
					// Re-notify daemon with correct connectionId
					s.notifyControls(map[string]interface{}{
						"type":          "connected",
						"connectionId":  ws.connectionId,
					})
				}
			}

			s.mu.Lock()
			serverWs := s.serverDataSockets[ws.connectionId]
			s.mu.Unlock()
			if serverWs == nil {
				s.bufferFrame(ws.connectionId, data)
				continue
			}
			serverWs.conn.WriteMessage(websocket.TextMessage, data)
		}
	}()
}

func (r *relay) flushFramesTo(ws *socket) {
	s := r.getSession(ws.serverId)
	s.flushFrames(ws.connectionId, ws)
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

	ws := &socket{
		conn:         conn,
		version:      version,
		role:         role,
		serverId:     serverId,
		connectionId: connectionId,
	}

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