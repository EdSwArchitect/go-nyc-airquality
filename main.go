package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	airquality "github.com/EdSwArchitect/go-nyc-airquality/data"
	elasticsearch "github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	// "github.com/elastic/go-elasticsearch/v6"
)

var wg sync.WaitGroup
var docs []airquality.AirQuality
var buf bytes.Buffer
var counter int

/*
type AirQuality struct {
	IndicatorDataID  int32
	IndicatorID      int32
	Name             string
	Measure          string
	GeoTypeName      string
	GeoEntityID      int32
	GeoEntityName    string
	YearDescription  string
	DataValueMessage float32
}
*/

func bulkIndexIt(elastic *elasticsearch.Client, record []string) {
	var doc airquality.AirQuality
	var res *esapi.Response
	var raw map[string]interface{}

	index := "nyc-air-quality"

	i, _ := strconv.ParseInt(record[0], 10, 32)

	doc.IndicatorDataID = int32(i)

	i, _ = strconv.ParseInt(record[1], 10, 32)

	doc.IndicatorID = int32(i)

	doc.Name = record[2]
	doc.Measure = record[3]
	doc.GeoTypeName = record[4]

	i, _ = strconv.ParseInt(record[5], 10, 32)
	doc.GeoEntityID = int32(i)

	doc.GeoEntityName = record[6]

	doc.YearDescription = record[7]

	f, _ := strconv.ParseFloat(record[8], 32)
	doc.DataValueMessage = float32(f)

	docs = append(docs, doc)

	// Prepare the metadata payload
	//
	meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, doc.IndicatorDataID, "\n"))
	fmt.Printf("%s\n", meta) // <-- Uncomment to see the payload

	// Prepare the data payload: encode article to JSON
	//
	data, err := json.Marshal(doc)

	if err != nil {
		log.Fatalf("Cannot encode article %d: %s", doc.IndicatorDataID, err)
	}

	// Append newline to the data payload
	//
	data = append(data, "\n"...) // <-- Comment out to trigger failure for batch

	// Append payloads to the buffer (ignoring write errors)
	//
	buf.Grow(len(meta) + len(data))
	buf.Write(meta)
	buf.Write(data)

	counter++

	if counter%100 == 0 {

		res, err = elastic.Bulk(bytes.NewReader(buf.Bytes()), elastic.Bulk.WithIndex(index))

		if err != nil {
			log.Fatalf("Failure indexing batch: %s", err)
		}

		// If the whole request failed, print error and mark all documents as failed
		//
		if res.IsError() {

			// numErrors += numItems

			if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
				log.Fatalf("Failure to to parse response body: %s", err)
			} else {
				log.Printf("  Error: [%d] %s: %s",
					res.StatusCode,
					raw["error"].(map[string]interface{})["type"],
					raw["error"].(map[string]interface{})["reason"],
				)
			}
			// A successful response might still contain errors for particular documents...
			//
		} else {
			fmt.Printf("It should have bulked indexed. Not checking individual bulk errors")
		}
	} // if counter%100 == 0 {
}

func indexIt(elastic *elasticsearch.Client, record []string) {
	defer wg.Done()

	index := "nyc-air-quality"

	var indexBody string

	indexBody = `{"indicator_data_id" : "` + record[0] + `", "indicator_id" : "` + record[1] + `",
	"name": "` + record[2] + `", "measure" : "` + record[3] + `", "geo_type_name": "` + record[4] + `", 
	"geo_entity_id" : "` + record[5] + `", "geo_entity_name" : "` + record[6] + `", "year_description" : "` + record[7] + `" , 
	"data_value_message" : "` + record[8] + `"}`

	// fmt.Printf("The body: '%s'\n", indexBody)

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: record[0],
		Body:       strings.NewReader(indexBody),
		// Body: strings.NewReader(`{"indicator_data_id" : "` + record[1] + `", "indicator_id" : "` + record[2] + `",
		// 	 "name": "` + record[3] + `", "measure" : "` + record[4] + `", "geo_type_name": "` + record[5] + `"}`),
		Refresh: "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), elastic)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error indexing document ID=%s - %s", res.Status(), record[0], indexBody)
	}
	// } else {
	// Deserialize the response into a map.
	// 	var r map[string]interface{}
	// 	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
	// 		log.Printf("Error parsing the response body: %s", err)
	// 	} else {
	// 		// Print the response status and indexed document version.
	// 		log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
	// 	}
	// }

	// fmt.Println(record)
	// }

}

func readCsv(elastic *elasticsearch.Client) {

	fileName := "/home/edbrown/go/src/github.com/EdSwArchitect/go-nyc-airquality/resources/Air_Quality.csv"

	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	excel := csv.NewReader(file)

	_, _ = excel.Read()

	// index := "nyc-air-quality"

	// var indexBody string

	for {
		record, err := excel.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// indicator_data_id, indicator_id, name, Measure, geo_type_name, geo_entity_id, geo_entity_name, year_description, data_valuemessage

		// wg.Add(1)

		fmt.Printf("Indexing record: %s\n", record[0])

		bulkIndexIt(elastic, record)
		// indexIt(elastic, record)

		// wg.Done()

	} // for {

}

func main() {
	fmt.Println("Hi, Ed")

	// var wg sync.WaitGroup

	var r map[string]interface{}

	elastic, _ := elasticsearch.NewDefaultClient()

	res, err := elastic.Info()

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	// Check response status
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}

	// Check response status
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	// Print client and server version numbers.
	log.Printf("Client: %s", elasticsearch.Version)
	log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])

	log.Printf("Results: %s", r)

	readCsv(elastic)

	// req := esapi.IndexRequest{
	// 	Index:      "test",
	// 	DocumentID: strconv.Itoa(1),
	// 	Body:       strings.NewReader(`{"title" : "Gumby"}`),
	// 	Refresh:    "true",
	// }

	// myres, err := req.Do(context.Background(), elastic)
	// if err != nil {
	// 	log.Fatalf("Error getting response: %s", err)
	// }
	// defer res.Body.Close()

	// if res.IsError() {
	// 	log.Printf("[%s] Error indexing document ID=1", myres.Status())
	// } else {
	// 	log.Printf("No error")
	// }

	// log.Println(strings.Repeat("~", 37))

	log.Println("End of test")

	// wg.Wait()
}
