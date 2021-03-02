package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-memdb"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type counters struct {
	sync.Mutex
	View  int `json:"view"`
	Click int `json:"click"`
}

var (
	//A map of map of counters pointers
	allCounters = map[string]map[string]*counters{}

	//All 4 possible types of content
	content = []string{"sports", "entertainment", "business", "education"}

	//The in-memory database
	db *memdb.MemDB

	err error

	//The limiter
	limiter = rate.NewLimiter(1, 3)
)

type counter struct {
	Time    string
	Content string
	View    int
	Click   int
}

//The schema for storage
var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		"counters": {
			Name: "counters",
			Indexes: map[string]*memdb.IndexSchema{
				"id": {
					Name:    "id",
					Unique:  true,
					Indexer: &memdb.StringFieldIndex{Field: "Time"},
				},
				"content": {
					Name:    "content",
					Unique:  false,
					Indexer: &memdb.IntFieldIndex{Field: "Content"},
				},
			},
		},
	},
}

func welcomeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Welcome to EQ Works 😎")
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	data := content[rand.Intn(len(content))]

	//If counter can be found, increment the view field by 1
	//If counter can't be found (e.g. when the content was visited for the 1st time in the time period),
	//then create a new counter and add to the map
	if counterMap, contentFound := allCounters[data]; contentFound {
		if counter, counterFound := counterMap[time.Now().Format(time.RFC822Z)]; counterFound {
			counter.Lock()
			counter.View++
			counter.Unlock()
			counterMap[time.Now().Format(time.RFC822Z)] = counter
		} else {
			counterMap[time.Now().Format(time.RFC822Z)] = &counters{
				View:  1,
				Click: 0,
			}
		}
	}

	err := processRequest(r)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(400)
		return
	}

	// simulate random click call
	if rand.Intn(100) < 50 {
		processClick(data)
	}

	w.WriteHeader(200)
	fmt.Fprintf(w, "you clicked a \"%s\" type page", data) //Simple message for debugging
	return

}

func processRequest(r *http.Request) error {
	time.Sleep(time.Duration(rand.Int31n(50)) * time.Millisecond)
	return nil
}

func processClick(data string) error {
	// Look for the right counter, if counter can be found, increment the click field by 1
	// If counter can't be found, then create the counter and add to the map
	if counterMap, contentFound := allCounters[data]; contentFound {
		if counter, counterFound := counterMap[time.Now().Format(time.RFC822Z)]; counterFound {
			counter.Lock()
			counter.Click++
			counter.Unlock()
			counterMap[time.Now().Format(time.RFC822Z)] = counter
		} else {
			counterMap[time.Now().Format(time.RFC822Z)] = &counters{
				Click: 1,
				View:  0,
			}
		}
	}

	return nil
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	if isAllowed() {
		//Get the 1st parameter, the type of content
		content, contentOK := r.URL.Query()["content"] 

		//If either parameter is missing, return a 400 bad request
		if !contentOK || len(content[0]) < 1 {
			http.Error(w, "Url Param 'content' is missing", http.StatusBadRequest)
			return
		}

		//Get the 2nd parameter, the time in RFC822Z format
		time, timeOK := r.URL.Query()["time"]
		if !timeOK || len(time[0]) < 1 {
			http.Error(w, "Url Param 'time' is missing", http.StatusBadRequest)
			return
		}

		//If the counter can be found, return the JSON representation of it
		//Otherwise return a 404
		if counterMap, contentFound := allCounters[content[0]]; contentFound {
			if counter, counterFound := counterMap[time[0]]; counterFound {
				fmt.Println(counter.Click)
				fmt.Println(counter.View)

				countersJSON, _ := json.Marshal(counter)
				fmt.Println(string(countersJSON))
				fmt.Fprintln(w, string(countersJSON))
			} else {
				http.Error(w, "counter not found :(", http.StatusNotFound)
			}
		}

		return
	} else {
		http.Error(w, "you're too fast, please wait :)", http.StatusTooManyRequests)
		return
	}
}

func isAllowed() bool {
	return limiter.Allow() //The limiter will restrict the rate of access
}

func uploadCounters(t time.Time) error {
	txn := db.Txn(true)
	for content, counters := range allCounters {
		for time, c := range counters {
			c.Lock()
			temp := counter{time, content, c.Click, c.View}
			c.Unlock()
			if err := txn.Insert("counters", temp); err != nil {
				panic(err)
			}
		}
	}
	txn.Commit()
	return nil
}

func doEvery(d time.Duration, f func(time.Time) error) {
	for x := range time.Tick(d) {
		f(x)
	}
}

func main() {
	db, err = memdb.NewMemDB(schema) //Create a new DB at the beginning
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(content); i++ {
		allCounters[content[i]] = map[string]*counters{}
	}

	http.HandleFunc("/", welcomeHandler)
	http.HandleFunc("/view/", viewHandler)
	http.HandleFunc("/stats/", statsHandler)

	//doEvery(50*time.Millisecond, uploadCounters)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
