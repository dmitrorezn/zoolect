package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var zkServers = []string{
	"localhost:2181",
	"localhost:2182",
	"localhost:2183",
}

type Cmd struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type SyncCache struct {
	mut   sync.RWMutex
	cache ICache
}

func NewSyncCache() *SyncCache {
	return &SyncCache{
		cache: NewCache(),
	}
}

func NewCache() *Cache {
	return &Cache{
		m: make(map[string]string),
	}
}

func (c *SyncCache) Set(key, val string) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	return c.cache.Set(key, val)
}

func (c *SyncCache) Get(key string) (string, error) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	return c.cache.Get(key)
}

type ICache interface {
	Set(key, val string) error
	Get(key string) (string, error)
}

type Cache struct {
	m map[string]string
}

func (c *Cache) Set(key, val string) error {
	c.m[key] = val

	return nil
}

var ErrNotFound = errors.New("not found")

func (c *Cache) Get(key string) (string, error) {
	v, ok := c.m[key]
	if !ok {
		return "", ErrNotFound
	}
	return v, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var addr = os.Getenv("HOST")

	// Attempt connection with Zookeeper.
	conn, _, err := zk.Connect(zkServers, time.Second*5)
	if err != nil {
		panic(err)
	}
	fmt.Println("host", addr)
	// Create the required nodes for election.
	myNodePath, err := createNodes(conn, addr)
	if err != nil {
		panic(err)
	}

	fmt.Println("My node path:", myNodePath)

	var (
		cache       = NewSyncCache()
		settersChan = make(chan Cmd, 1048)
	)

	db, err := Connect()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var repository = Repository{
		db: db,
	}
	set := func(cmd Cmd) {
		fmt.Println("SET", cmd)
		var err error
		if err = repository.Insert(ctx, cmd); err != nil {
			fmt.Println("Insert", err)
		}
		if err = cache.Set(cmd.Key, cmd.Value); err != nil {
			fmt.Println("Set", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/replicate", func(writer http.ResponseWriter, request *http.Request) {
		var replicateLog Replicate
		_ = json.NewDecoder(request.Body).Decode(&request)
		fmt.Println(" replicate", request)
		switch replicateLog.Command {
		case "set":
			set(replicateLog.Cmd)
		}
	})
	var election Election

	mux.HandleFunc("/set", func(writer http.ResponseWriter, request *http.Request) {
		if ctx.Err() != nil {
			http.Error(writer, ctx.Err().Error(), http.StatusInternalServerError)
			return
		}
		var cmd Cmd
		_ = json.NewDecoder(request.Body).Decode(&cmd)
		fmt.Println("SET CMD", cmd)
		isLeader := election.Leader != "" && election.Leader == addr
		if isLeader {
			set(cmd)
			settersChan <- cmd
		}
		if !isLeader {
			if _, err = NewClient(election.Leader, Jsoner{}).Replicate(ctx, Replicate{"set", cmd}); err != nil {
				fmt.Println("Replicate CMD err", cmd)
			}
		}
		fmt.Println("Replicated CMD", cmd)

		return
	})
	type Result struct {
		Value string `json:"value"`
	}
	mux.HandleFunc("/get", func(writer http.ResponseWriter, request *http.Request) {
		var cmd Cmd
		_ = json.NewDecoder(request.Body).Decode(&cmd)
		v, err := cache.Get(cmd.Key)
		if err == nil {
			_ = json.NewEncoder(writer).Encode(Result{
				Value: v,
			})

			return
		}
		if !errors.Is(err, ErrNotFound) {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}
		v, err = repository.Find(ctx, cmd.Key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(writer, err.Error(), http.StatusNotFound)
				return
			}

			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		err = cache.Set(cmd.Key, cmd.Value)

		return
	})

	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if errL := server.ListenAndServe(); errL != nil && !errors.Is(errL, http.ErrServerClosed) {
			panic("ListenAndServe" + errL.Error())
		}
	}()
	shutdown := func() {
		close(settersChan)
	}
	shutdown = intercept(
		shutdown,
		func() {
			fmt.Println("intercept -> Shutdown", err)
			if errS := server.Shutdown(ctx); errS != nil {
				fmt.Println("Shutdown", err)
			}
		},
		func() {
			fmt.Println("CLOSED")
		})
	shutdown = intercept(shutdown, func() {
		fmt.Println("CLOSED")
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdown()
	}()

	defer func() {
		wg.Wait()
		fmt.Println("STOPPED")
	}()

	// Block until elected as leader.
	nodes, err := election.awaitVictory(ctx, conn, myNodePath)
	if err != nil {
		fmt.Println("awaitVictory", err)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				break
			case cmd := <-settersChan:
				if ctx.Err() != nil {
					return
				}
				fmt.Println("cmd", cmd)
				for _, addr := range nodes {
					if ctx.Err() != nil {
						return
					}
					if _, err = NewClient("localhost"+addr, Jsoner{}).Replicate(ctx, Replicate{"set", cmd}); err != nil {
						fmt.Println("Replicate to ", addr, "err", err.Error())
					} else {
						fmt.Println("Replicated to ", addr)
					}
				}
			}
		}
	}()

	fmt.Println("Elected as leader.")
}

type Client struct {
	addr    string
	Builder Builder
	*http.Client
}

func NewClient(addr string, Builder Builder) *Client {
	return &Client{
		addr:    addr,
		Builder: Builder,
		Client:  http.DefaultClient,
	}
}

type Jsoner struct {
}

func (Jsoner) BuildBody(payload interface{}) (io.Reader, error) {
	b := new(bytes.Buffer)

	return b, json.NewEncoder(b).Encode(payload)
}

type Builder interface {
	BuildBody(payload interface{}) (io.Reader, error)
}

func buildURI(addr, path string) string {
	return fmt.Sprintf("http://%s%s", addr, path)
}

func (c *Client) Set(ctx context.Context, cmd Cmd) ([]byte, error) {
	body, err := c.Builder.BuildBody(cmd)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, buildURI(c.addr, "/set"), body)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Do(r)
	if err != nil {
		return nil, err
	}
	response, err := io.ReadAll(resp.Body)
	if err = errors.Join(err, resp.Body.Close()); err != nil {
		return nil, err
	}

	return response, err
}

type Replicate struct {
	Command string `json:"command"`
	Cmd     Cmd    `json:"cmd"`
}

func (c *Client) Replicate(ctx context.Context, replicate Replicate) ([]byte, error) {
	body, err := c.Builder.BuildBody(replicate)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, buildURI(c.addr, "/set"), body)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Do(r)
	if err != nil {
		return nil, err
	}
	response, err := io.ReadAll(resp.Body)
	if err = errors.Join(err, resp.Body.Close()); err != nil {
		return nil, err
	}

	return response, err
}
func intercept(f func(), fn ...func()) func() {
	return func() {
		f()
		for _, f = range fn {
			f()
		}
	}
}
