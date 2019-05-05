package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

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

	// i, _ := strconv.ParseInt(record[0], 10, 32)

	doc.IndicatorDataID = record[0]

	// i, _ = strconv.ParseInt(record[1], 10, 32)

	doc.IndicatorID = record[1]

	doc.Name = record[2]
	doc.Measure = record[3]
	doc.GeoTypeName = record[4]

	// i, _ = strconv.ParseInt(record[5], 10, 32)
	doc.GeoEntityID = record[5]

	doc.GeoEntityName = record[6]

	doc.YearDescription = record[7]

	// f, _ := strconv.ParseFloat(record[8], 32)
	// doc.DataValueMessage = float32(f)
	doc.DataValueMessage = record[8]

	docs = append(docs, doc)

	// Prepare the metadata payload
	//
	// meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, doc.IndicatorDataID, "\n"))
	meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, uuid.New().String(), "\n"))
	// fmt.Printf("%s\n", meta) // <-- Uncomment to see the payload

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

	//var bulk esapi.Bulk()

	counter++
	if counter%150 == 0 {

		res, err = elastic.Bulk(bytes.NewReader(buf.Bytes()), elastic.Bulk.WithIndex(index), elastic.Bulk.WithDocumentType("mydoc"), elastic.Bulk.WithRefresh("wait_for"))

		if err != nil {
			log.Fatalf("Failure indexing batch: %s", err)
		} else {

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
			}

			time.Sleep(1 * time.Second)
		}

		buf.Reset()

		// else {
		// 	fmt.Printf("It should have bulked indexed. Not checking individual bulk errors")
		// }
	} // if counter%100 == 0 {

	// res, err = elastic.Bulk(bytes.NewReader(buf.Bytes()), elastic.Bulk.WithIndex(index), elastic.Bulk.WithDocumentType("mydoc"), elastic.Bulk.WithRefresh("wait_for"))

	// if err != nil {
	// 	log.Fatalf("Last failure indexing batch: %s", err)
	// }

	// fmt.Printf("Last one. %+v\n", res)

}

func finishBulk(elastic *elasticsearch.Client) {
	index := "nyc-air-quality"

	_, err := elastic.Bulk(bytes.NewReader(buf.Bytes()), elastic.Bulk.WithIndex(index), elastic.Bulk.WithDocumentType("mydoc"), elastic.Bulk.WithRefresh("wait_for"))

	if err != nil {
		log.Fatalf("Last failure indexing batch: %s", err)
	}

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

func readCsv(elastic *elasticsearch.Client, fileName string) {

	// fileName := "/home/edbrown/go/src/github.com/EdSwArchitect/go-nyc-airquality/resources/Air_Quality.csv"
	// fileName := "/home/edbrown/go/src/github.com/EdSwArchitect/go-nyc-airquality/resources/Air_Quality_First.csv"
	// fileName := "/home/edbrown/go/src/github.com/EdSwArchitect/go-nyc-airquality/resources/Air_Quality_Last.csv"

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

		// fmt.Printf("Indexing record: %s\n", record[0])

		bulkIndexIt(elastic, record)
		// indexIt(elastic, record)

		// wg.Done()

	} // for {

	finishBulk(elastic)

}

func search(es *elasticsearch.Client, value *string, field airquality.SearchField) {

	// Name SearchField = iota + 1
	// Measure
	// GeoTypeName
	// GeoEntityID
	// GeoEntityName
	// YearDescription
	// DataValueMessage

	// Name             string `json:"name"`
	// Measure          string `json:"measure"`
	// GeoTypeName      string `json:"geo_type_name"`
	// GeoEntityID      string `json:"geo_entity_id"`
	// GeoEntityName    string `json:"geo_entity_name"`
	// YearDescription  string `json:"year_description"`
	// DataValueMessage string `json:"data_value_message"`

	var query string

	switch field {
	case airquality.Undef:
		query = `{"query" : { "match_all" : {} }}`
	case airquality.Name:
		query = `{"query" : { "match" : { "name" : "` + *value + `" } }}`
	case airquality.Measure:
		query = `{"query" : { "match" : { "measure" : "` + *value + `" } }}`
	case airquality.GeoTypeName:
		query = `{"query" : { "match" : { "geo_type_name" : "` + *value + `" } }}`
	case airquality.GeoEntityID:
		query = `{"query" : { "match" : { "geo_entity_id" : "` + *value + `" } }}`
	case airquality.GeoEntityName:
		query = `{"query" : { "match" : { "geo_entity_name" : "` + *value + `" } }}`
	case airquality.YearDescription:
		query = `{"query" : { "match" : { "year_description" : "` + *value + `" } }}`
	case airquality.DataValueMessage:
		query = `{"query" : { "match" : { "data_value_message" : "` + *value + `" } }}`
	default:
		query = `{"query" : { "match_all" : {} }}`
	}

	// 3. Search for the indexed documents
	//
	// Use the helper methods of the client.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("nyc-air-quality"),
		es.Search.WithBody(strings.NewReader(query)),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
		es.Search.WithHuman(),
		es.Search.WithSize(500),
		es.Search.WithFrom(0),
	)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var r map[string]interface{}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)
	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	}

	log.Println(strings.Repeat("=", 37))
}

func main() {

	cmd := flag.String("command", "---", "Command. Either 'query' or 'load'")

	field := flag.String("field", "---", "Query field")

	queryValue := flag.String("value", "---", "Query field value")

	file := flag.String("file", "---", "The path to the file to load")

	flag.Parse()

	fmt.Printf("Command '%s'\nField '%s'\nQuery value '%s'\n", *cmd, *field, *queryValue)

	// var wg sync.WaitGroup

	var r map[string]interface{}

	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			DialContext:         (&net.Dialer{Timeout: 60 * time.Second}).DialContext,
		},
	}

	// elastic, _ := elasticsearch.NewDefaultClient()
	elastic, _ := elasticsearch.NewClient(cfg)

	res, err := elastic.Info()

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
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

	if *cmd == "query" {

		fieldName := airquality.GetField(*field)

		search(elastic, queryValue, fieldName)
	} else if *cmd == "load" {

		if _, err := os.Stat(*file); os.IsNotExist(err) {

			fmt.Printf("File %s doesn't exist.\n", *file)

		} else {

			fmt.Printf("Loading %s into ElasticSearch\n", *file)

			readCsv(elastic, *file)
		}

	} else {
		log.Printf("bad command: '%s'", *cmd)
	}

	log.Println("End of test")

	// wg.Wait()
}
