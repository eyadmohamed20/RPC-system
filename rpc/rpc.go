// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}


// Start begins accepting remote calls at address. Use port 0 to request an available port.
// Start begins accepting remote calls at address. Use port 0 to request an available port.
func (s *Server) Start(address string) error {
	s.mu.Lock()

	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("server is already running")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.listener = listener
	s.stopped = make(chan struct{})
	s.acceptDone = make(chan struct{})

	s.mu.Unlock()

	started := make(chan struct{})
	go s.acceptLoop(started)
	<-started

	return nil
}
// acceptLoop accepts connections until Stop signals it to quit. Every accepted
// connection is handed to its own goroutine so slow clients cannot block others.
// acceptLoop accepts connections until Stop signals it to quit. Every accepted
// connection is handed to its own goroutine so slow clients cannot block others.
func (s *Server) acceptLoop(started chan<- struct{}) {
	// Snapshot all shared fields ONCE, under the mutex, before anything else.
	// Stop may null the fields at any time; the loop must not read them again.
	s.mu.Lock()
	listener := s.listener
	stopped := s.stopped
	acceptDone := s.acceptDone
	s.mu.Unlock()

	close(started) // signal Start that the snapshot is complete

	defer close(acceptDone)

	for {
		connection, err := listener.Accept()
		if err != nil {
			select {
			case <-stopped:
				// Stop() closed us down. Normal exit.
				return
			default:
				// Transient error (fd exhaustion, etc.). Brief backoff, then retry.
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		s.workers.Add(1)
		go func(conn net.Conn) {
			defer s.workers.Done()
			s.handle(conn)
		}(connection)
	}
}



// Address returns the address assigned to the running server.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop stops accepting new connections and waits for active handlers to finish.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.listener == nil {
		s.mu.Unlock()
		return nil // never started, or already stopped
	}

	// Snapshot the per-start state and immediately mark the server stopped so
	// that a concurrent Address() or Start() sees a clean state.
	listener := s.listener
	stopped := s.stopped
	acceptDone := s.acceptDone

	s.listener = nil
	s.stopped = nil
	s.acceptDone = nil
	s.mu.Unlock()

	// 1. Signal the accept loop so it knows the next Accept error is intentional.
	close(stopped)

	// 2. Close the listener so Accept() returns and the port is released.
	err := listener.Close()

	// 3. Wait for the accept loop to finish; no new handlers can be spawned after this.
	<-acceptDone

	// 4. Wait for in-flight handlers to finish.
	s.workers.Wait()

	return err
}

// handle reads one request, invokes it, and writes one response.
func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	encoder := json.NewEncoder(connection)

	send := func(r response) {
		_ = connection.SetWriteDeadline(time.Now().Add(defaultCallTimeout))
		_ = encoder.Encode(r)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			send(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	_ = connection.SetReadDeadline(time.Now().Add(defaultCallTimeout))

	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+1)))
	var req request
	if err := decoder.Decode(&req); err != nil {
		send(response{Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}
	if err := validateRequest(req); err != nil {
		send(response{Error: err.Error()})
		return
	}
	result, err := s.invoke(req)
	if err != nil {
		send(response{Error: err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		send(response{Error: fmt.Sprintf("encode result: %v", err)})
		return
	}
	send(response{OK: true, Value: payload})
}

// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	for _, char := range req.Method {
		if unicode.IsSpace(char) || unicode.IsControl(char) {
			return errors.New("method name contains whitespace or control characters")
		}
	}

	if len(req.Args) > maxArguments {
		return errors.New("too many arguments")
	}

	totalBytes := 0
	for _, arg := range req.Args {
		if len(arg) > maxArgumentBytes {
			return errors.New("argument is larger than maxArgumentBytes")
		}
		totalBytes += len(arg)
	}

	if totalBytes > maxRequestBytes {
		return errors.New("combined arguments are larger than maxRequestBytes")
	}

	return nil
}

// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	typeOfMethod := method.Type()

	if typeOfMethod.IsVariadic() {
		return nil, fmt.Errorf("method %q is variadic, which is not supported", req.Method)
	}
	if n := typeOfMethod.NumOut(); n > 2 || (n == 2 && !typeOfMethod.Out(1).Implements(errorType)) {
		return nil, fmt.Errorf("method %q has an unsupported return shape", req.Method)
	}

	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	outputs, err := callMethodSafely(method, arguments)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(errorType) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// Call invokes method and decodes its result into out. Pass nil for no result.
func (c *Client) Call(method string, out any, args ...any) error {
	if method == "" {
		return errors.New("method cannot be empty")
	}

	conn, err := net.DialTimeout("tcp", c.Address, c.Timeout)
	if err != nil {
		return fmt.Errorf("TCP connection failed: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(c.Timeout)); err != nil {
		return fmt.Errorf("failed to set deadline: %w", err)
	}

	var encodedArgs []json.RawMessage
	for i, arg := range args {
		marshaledArg, err := json.Marshal(arg)
		if err != nil {
			return fmt.Errorf("failed to encode argument %d: %w", i, err)
		}
		encodedArgs = append(encodedArgs, marshaledArg)
	}

	req := request{
		Method: method,
		Args:   encodedArgs,
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	var res response
	if err := json.NewDecoder(conn).Decode(&res); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !res.OK {
		if res.Error != "" {
			return errors.New(res.Error)
		}
		return errors.New("remote call failed")
	}

	if out != nil && len(res.Value) > 0 {
		if err := json.Unmarshal(res.Value, out); err != nil {
			return fmt.Errorf("failed to decode result into out type: %w", err)
		}
	}

	return nil
}