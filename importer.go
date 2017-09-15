package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

//OpenTSDBUrl is the URL of OpenTSDB import interface
var OpenTSDBUrl string

//FolderScanInterval is the time interval to scan the metric data file folder
var FolderScanInterval time.Duration

//ImportBatchSize is the max number of metrics in a OpenTSDB import(POST) operation
var ImportBatchSize int

//coordinate is responsible to scan the metric data files located folder and sends the new filename to todo channel (for file importing)
func coordinate(wg *sync.WaitGroup, todo chan<- string, done <-chan string, quit <-chan int) {
	wg.Add(1)
	toimp := make(map[string]string, 1000)
	t1 := time.NewTimer(FolderScanInterval)

	for {
		select {
		case <-t1.C:
			t1.Stop()
			//Scan the folder for new file.
			files, err := filepath.Glob("*.gz")
			if err != nil {
				log.Println("Failed to scan *.gz files due to " + err.Error())
				continue
			}

			//Sends new files to todo channel
			for _, f := range files {
				_, ok := toimp[f]
				if !ok {
					toimp[f] = f
					todo <- f
				}
			}

			t1.Reset(FolderScanInterval)
		case f := <-done:
			//Kicks off the imported filename from map.
			delete(toimp, f)
		case <-quit:
			t1.Stop()
			log.Println("Coordinator exits!")
			wg.Done()
			return
		}
	}

}

func importFile(id int, wg *sync.WaitGroup, todo <-chan string, done chan<- string, quit <-chan int) {

	wg.Add(1)
	var filename string

	for {
		select {
		case filename = <-todo:
			file, err := os.Open(filename)
			if err != nil {
				log.Println("Failed to open the metric data file named " + filename)
				continue
			}
			defer file.Close()

			gunzipper, err := gzip.NewReader(file)
			if err != nil {
				log.Println("Failed to open the metric data file named " + filename)
				continue
			}
			defer gunzipper.Close()

			reader := bufio.NewReader(gunzipper)

			buf := new(bytes.Buffer)
			lineNo := 0
			isPartial := false

			for {
				line, isPrefix, err := reader.ReadLine()
				if err != nil && err != io.EOF {
					log.Println("Failed to read line from " + filename + " due to " + err.Error())
					break
				}

				if err != nil && err == io.EOF {
					if lineNo%ImportBatchSize > 0 {
						buf.WriteString("\n]")
						postToOpenTSDB(buf)
					}
					done <- filename
					break
				}

				if len(line) == 0 { //end of file
					continue
				}

				if !isPartial && isPrefix { //first part
					lineNo++
					if lineNo%ImportBatchSize == 1 { //first json
						buf.WriteString("[\n")
					}
					if lineNo%ImportBatchSize > 1 {
						buf.WriteString(",\n")
					}
					buf.Write(line)

					isPartial = true

				} else if isPartial && !isPrefix { //last part
					buf.Write(line)
					if lineNo%ImportBatchSize == 0 {
						buf.WriteString("\n]")
						postToOpenTSDB(buf)
					}
					isPartial = false

				} else if isPartial && isPrefix { //middle parts
					buf.Write(line)
				} else if !isPartial && !isPrefix { //or a whole line
					lineNo++
					if lineNo%ImportBatchSize == 1 { //first json
						buf.WriteString("[\n")
					}
					if lineNo%ImportBatchSize > 1 {
						buf.WriteString(",\n")
					}
					buf.Write(line)

					if lineNo%ImportBatchSize == 0 {
						buf.WriteString("\n]")
						postToOpenTSDB(buf)
					}
				}

			}

		case <-quit:
			log.Println("File importer #" + strconv.Itoa(id) + " exits!")
			wg.Done()
			return
		}
	}

}

func postToOpenTSDB(buf *bytes.Buffer) {

	defer buf.Reset()

	resp, err := http.Post(OpenTSDBUrl, "application/json", buf)
	if err != nil {
		log.Println("Failed to post the metrics into OpenTSDB.")
		log.Printf("Metrics: \n %s \n", buf.String())
		return
	}

	if resp.StatusCode != 200 {
		log.Println("Failed to post the metrics into OpenTSDB.")
		log.Printf("Metrics: \n %s \n", buf.String())
	}

	//More verification logic could be added here...

}

func main() {

	//Initialization
	OpenTSDBUrl = "10.99.73.32:4242/api/put?details"
	FolderScanInterval = time.Second * 1
	ImportBatchSize = 100

	var (
		//Channel for filename to import
		todo = make(chan string, 10000)

		//Channel for imported filename
		done = make(chan string, 10000)

		quit = make(chan int)

		wg = new(sync.WaitGroup)
	)

	rnum := 2
	go coordinate(wg, todo, done, quit)
	go importFile(1, wg, todo, done, quit)

	//Send quit to goroutines if any signals from OS for gracefully shutdown.
	sc := make(chan os.Signal)
	signal.Notify(sc, syscall.SIGINT, os.Interrupt, os.Kill)
	<-sc
	for i := 0; i < rnum; i++ {
		quit <- i
	}
	wg.Wait()
	log.Println("Importer exits! See you later!")
}
