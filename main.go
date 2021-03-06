package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/UniversityRadioYork/baps3-go"
	"github.com/docopt/docopt-go"
)

type server struct {
	Hostport string
}

type httpServer struct {
	Hostport string
}

// Config is a struct containing the configuration for an instance of Bifrost.
type Config struct {
	Servers map[string]server
	HTTP    httpServer
}

func killConnectors(connectors []*bfConnector) {
	for _, c := range connectors {
		close(c.reqCh)
	}
}
func parseArgs() (args map[string]interface{}, err error) {
	usage := `heimdallr.

Usage:
  heimdallr [-c <configfile>]
  heimdallr -h
  heimdallr -v

Options:
  -c --config=<configfile>    Path to heimdallr config file [default: config.toml].
  -h --help                   Show this help message.
  -v --version                Show version.`

	args, err = docopt.Parse(usage, nil, true, "heimdallr 0.0", false)
	return
}

func main() {
	logger := log.New(os.Stdout, "[-] ", log.Lshortfile)
	args, err := parseArgs()
	if err != nil {
		logger.Fatal("Error parsing args: " + err.Error())
	}
	conffile, err := ioutil.ReadFile(args["--config"].(string))
	if err != nil {
		logger.Fatal(err)
	}
	var conf Config
	if _, err := toml.Decode(string(conffile), &conf); err != nil {
		logger.Fatal(err)
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	resCh := make(chan baps3.Message)

	connectors := []*bfConnector{}

	wg := new(sync.WaitGroup)

	for name, s := range conf.Servers {
		c := initBfConnector(name, resCh, wg, logger)
		connectors = append(connectors, c)
		c.conn.Connect(s.Hostport)
		go c.Run()
	}

	// Goroutine for the heimdallr connector, and the lower-level
	// baps3-go connector.
	wg.Add(len(connectors) * 2)
	wspool := NewWspool(wg)
	initAndStartHTTP(conf.HTTP, connectors, wspool, logger)
	go wspool.run()

	for {
		select {
		case data := <-resCh:
			fmt.Println(data.String())
			wspool.broadcast <- []byte(data.String())
		case <-sigs:
			killConnectors(connectors)
			close(wspool.broadcast)
			wg.Wait()
			logger.Println("Exiting...")
			os.Exit(0)
		}
	}
}

func initAndStartHTTP(conf httpServer, connectors []*bfConnector, wspool *Wspool, logger *log.Logger) {
	mux := initHTTP(connectors, wspool, logger)
	go func() {
		err := http.ListenAndServe(conf.Hostport, mux)
		if err != nil {
			logger.Println(err)
		}
	}()
}
