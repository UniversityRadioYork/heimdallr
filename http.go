package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type httpRequest struct {
	resource string
	method   string
	payload  []byte
	resCh    chan<- interface{}
}

func initHTTP(connectors []*bfConnector, wspool *Wspool, log *log.Logger) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}
		c := &wsConn{send: make(chan []byte, 256), ws: ws}
		wspool.register <- c
		c.writeLoop()
	})

	for i := range connectors {
		installConnector(r, connectors[i])
	}

	r.PathPrefix("/").Handler(http.FileServer(http.Dir("./static/")))

	return r
}

func installConnector(router *mux.Router, connector *bfConnector) {
	fn := func(w http.ResponseWriter, r *http.Request) {
		resCh := make(chan interface{})

		fmt.Printf("sending request to %s\n", connector.name)

		resource := r.URL.Path

		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		payload := buf.Bytes()

		w.Header().Add("Content-Type", "application/json")
		connector.reqCh <- httpRequest{
			resource,
			r.Method,
			payload,
			resCh,
		}

		select {
		case res := <-resCh:
			err := dumpJSON(w, res)
			if err != nil {
				fmt.Println(err)
				break
			}
		}
	}

	router.HandleFunc("/"+connector.name, fn)
	router.HandleFunc("/"+connector.name+"/", fn)
	router.PathPrefix("/" + connector.name + "/").HandlerFunc(fn)
}

// dumpJSON dumps the JSON marshalling of res into w.
func dumpJSON(w io.Writer, res interface{}) (err error) {
	j, err := json.Marshal(res)

	if err == nil {
		_, err = w.Write(j)
	}

	return
}
