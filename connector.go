package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/UniversityRadioYork/baps3-go"
)

// Response is the outer structure of all json responses.
type Response struct {
	Status string
	Value  interface{} `json:",omitempty"`
}

// GetOk creates a Response wrapping a successful GET result.
func GetOk(value interface{}) *Response {
	r := new(Response)
	r.Status = "ok"
	r.Value = value
	return r
}

// PutRequest is the structure of all incoming json PUT payloads
type PutRequest struct {
	Value interface{}
}

// PutOk creates a Response wrapping a successful PUT
func PutOk() *Response {
	r := new(Response)
	r.Status = "ok"
	return r
}

type bfConnector struct {
	conn   *baps3.Connector
	name   string
	wg     *sync.WaitGroup
	logger *log.Logger
	state  *baps3.ServiceState

	reqCh chan httpRequest
	resCh <-chan baps3.Message

	// TODO(CaptainHayashi): move this away from baps3.Message to
	// something generic.
	updateCh chan<- baps3.Message
}

func initBfConnector(name string, updateCh chan<- baps3.Message, wg *sync.WaitGroup, logger *log.Logger) (c *bfConnector) {
	resCh := make(chan baps3.Message)

	c = new(bfConnector)
	c.resCh = resCh
	c.conn = baps3.InitConnector(name, resCh, wg, logger)
	c.name = name
	c.wg = wg
	c.logger = logger
	c.reqCh = make(chan httpRequest)
	c.updateCh = updateCh
	c.state = baps3.InitServiceState()
	return
}

func (c *bfConnector) Run() {
	defer c.wg.Done()
	defer close(c.conn.ReqCh)

	go c.conn.Run()

	fmt.Printf("connector %s now listening for requests\n", c.name)

	for {
		select {
		case rq, ok := <-c.reqCh:
			if !ok {
				return
			}

			// TODO(CaptainHayashi): probably make this more robust
			resource := strings.Replace(rq.resource, "/"+c.name, "", 1)
			fmt.Printf("connector %s response %s\n", c.name, resource)

			// TODO(wlcx): A default and appropriate error for other methods
			// atm this results in stalled http request
			switch rq.method {
			case "GET":
				rq.resCh <- c.get(resource)
			case "PUT":
				rq.resCh <- c.put(resource, rq.payload)
			}
		case res := <-c.resCh:
			if err := c.state.Update(res); err != nil {
				fmt.Println(err)
			}
			c.updateCh <- res
		}
	}

	return
}

func splitResource(resource string) []string {
	res := strings.Split(strings.Trim(resource, "/"), "/")

	// The empty resource is returned as {""}: let's fix that
	if len(res) == 1 && res[0] == "" {
		res = []string{}
	}

	return res
}

func (c *bfConnector) get(resource string) interface{} {
	// TODO(CaptainHayashi): HTTP status codes

	resourcePath := splitResource(resource)

	r := c.rootGet(resourcePath)

	if r == nil {
		// TODO(CaptainHayashi): more errors
		return Response{
			Status: "what",
			Value:  "resource not found: " + resource,
		}
	}

	return GetOk(r)
}

func (c *bfConnector) put(resource string, payload []byte) interface{} {
	resourcePath := splitResource(resource)
	r := c.rootPut(resourcePath, payload)

	if r == nil {
		return Response{
			Status: "what",
			Value:  "resource not found: " + resource,
		}
	}

	return PutOk()
}

/* These resMaps describe simple composite resources, mapping each child
 * resource to the functions handling them.
 *
 * TODO(CaptainHayashi): decouple traversal from GET
 * TODO(CaptainHayashi): maybe make traversal iterative instead of recursive?
 */

type resHandler struct {
	get func(*bfConnector, []string) interface{}
	put func(*bfConnector, []string, []byte) interface{}
}

type resMap map[string]resHandler

var (
	rootRes = resMap{
		"control": resHandler{
			(*bfConnector).controlGet,
			(*bfConnector).controlPut,
		},
		"player": resHandler{(*bfConnector).playerGet, nil},
		// "playlist": resHandler{(*bfConnector).playlistGet, nil},
	}
	controlRes = resMap{
		"features": resHandler{(*bfConnector).featuresGet, nil},
		"state": resHandler{
			(*bfConnector).stateGet,
			(*bfConnector).statePut,
		},
	}
	playerRes = resMap{
		"time": resHandler{(*bfConnector).timeGet, nil},
		"file": resHandler{(*bfConnector).fileGet, nil},
	}
)

func (c *bfConnector) getResource(rm resMap, resourcePath []string) interface{} {
	if len(resourcePath) == 0 {
		// Pull down all of the available child resources in this
		// resource.
		object := make(map[string]interface{})

		for k := range rm {
			child := rm[k].get(c, []string{})

			// Only add a key if the child definitely exists.
			if child != nil {
				object[k] = child
			}
		}

		return object
	}

	// Does the next step on the resource path exist?
	rhandler, ok := rm[resourcePath[0]]
	if ok {
		// Make it that resource's responsibility to
		// find the resource, then.
		return rhandler.get(c, resourcePath[1:])
	}
	return nil
}

func (c *bfConnector) putResource(rm resMap, resourcePath []string, payload []byte) interface{} {
	if len(resourcePath) == 0 {
		return nil
	}

	// Does the next step on the resource path exist?
	rhandler, ok := rm[resourcePath[0]]
	if ok {
		// Make it that resource's responsibility to
		// find the resource, then.
		return rhandler.put(c, resourcePath[1:], payload)
	}
	return nil
}

// rootGet is the GET handler for the / resource.
func (c *bfConnector) rootGet(resourcePath []string) interface{} {
	return c.getResource(rootRes, resourcePath)
}

// rootPut is the PUT handler for the / resource.
func (c *bfConnector) rootPut(resourcePath []string, payload []byte) interface{} {
	return c.putResource(rootRes, resourcePath, payload)
}

// controlGet is the GET handler for the /control resource.
func (c *bfConnector) controlGet(resourcePath []string) interface{} {
	return c.getResource(controlRes, resourcePath)
}

// controlPut is the PUT handler for the /control resource.
func (c *bfConnector) controlPut(resourcePath []string, payload []byte) interface{} {
	return c.putResource(controlRes, resourcePath, payload)
}

// playerGet is the GET handler for the /player resource.
func (c *bfConnector) playerGet(resourcePath []string) interface{} {
	// TODO(CaptainHayashi): Probably a spec change, but the fact that this
	// resource is guarded by more than one feature is iffy.  Do we need a
	// Player feature?
	if !(c.state.HasFeature(baps3.FtFileLoad) || c.state.HasFeature(baps3.FtTimeReport)) {
		return nil
	}

	return c.getResource(playerRes, resourcePath)
}


// GET value for /control/features
func (c *bfConnector) featuresGet(resourcePath []string) interface{} {
	// We only want a resource length of 0 (all features), or 1
	// (some index into the list of resources).
	if 1 < len(resourcePath) {
		return nil
	}

	fstrings := []string{}

	for k := range c.state.Features {
		fstrings = append(fstrings, k.String())
	}

	// There's no need to sort these, but it doesn't hurt to make the list a
	// bit easier for humans to eyeball.
	sort.Strings(fstrings)

	// Did we want the whole resource (the features list)?
	if len(resourcePath) == 0 {
		return fstrings
	}

	// If not, we assume we were indexing into the features list.
	// TODO(CaptainHayashi): Factor this list-resource pattern out?
	//   It might be useful for playlists later.
	i, err := strconv.Atoi(resourcePath[0])
	// TODO(CaptainHayashi): handle err properly
	if err == nil && 0 <= i && i <= len(fstrings) {
		return fstrings[i]
	}

	return nil
}

// GET value for /control/state
func (c *bfConnector) stateGet(resourcePath []string) interface{} {
	if 0 < len(resourcePath) {
		return nil
	}

	return c.state.State
}

var stateMap = map[string]baps3.MessageWord{
	"play": baps3.RqPlay,
	"stop": baps3.RqStop,
}

// PUT function for /control/state
func (c *bfConnector) statePut(resourcePath []string, payload []byte) interface{} {
	if 0 < len(resourcePath) {
		return nil
	}

	var jsonReq PutRequest
	if err := json.Unmarshal(payload, &jsonReq); err != nil {
		// Do something, Gromit!
	}
	newstate, ok := jsonReq.Value.(string)
	if ok {
		rqWord, ok := stateMap[newstate]
		if ok {
			c.conn.ReqCh <- *baps3.NewMessage(rqWord)
		}
	}

	return c.state
}

// GET value for /player/time
func (c *bfConnector) timeGet(resourcePath []string) interface{} {
	if 0 < len(resourcePath) {
		return nil
	}
	if !c.state.HasFeature(baps3.FtTimeReport) {
		return nil
	}

	// Time is reported in _micro_seconds
	return c.state.Time.Nanoseconds() / 1000
}

// GET value for /player/file
func (c *bfConnector) fileGet(resourcePath []string) interface{} {
	if 0 < len(resourcePath) {
		return nil
	}
	if !c.state.HasFeature(baps3.FtFileLoad) {
		return nil
	}

	return c.state.File
}
