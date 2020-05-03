package main

import (
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

func WithDatabase(op func(context.Context, *mongo.Collection)) {
	// Retrieve connection URI
	connecturi := os.Getenv("AZURE_COSMOSDB_CONNECTION_STRING")

	// Create a context to use with the connection
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	// Connect to the DB
	config := options.Client().ApplyURI(connecturi).SetRetryWrites(false)
	client, err := mongo.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}

	// Ping the DB to confirm the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database("home").Collection("sensors")

	// Perform DB operation
	op(ctx, collection)

	// Close the connection
	err = client.Disconnect(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

type TemperatureReading struct {
	Room        string
	Time        string
	Temperature float64
	Humidity    float64
}

func setTemperature(rw http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		panic(err)
	}

	var t TemperatureReading
	err = json.Unmarshal(body, &t)
	if err != nil {
		panic(err)
	}

	log.Println(t.Temperature)

	WithDatabase(func(ctx context.Context, collection *mongo.Collection) {
		_, err := collection.InsertOne(ctx, t)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func getTemperature(rw http.ResponseWriter, req *http.Request) {

	WithDatabase(func(ctx context.Context, collection *mongo.Collection) {
		filter := bson.D{{"room", "test"}}
		opts := options.FindOne().SetSort(bson.D{{"time", -1}})
		var t TemperatureReading
		err := collection.FindOne(ctx, filter, opts).Decode(&t)
		if err != nil {
			log.Fatal(err)
		}

		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(t)
	})
}

func main() {
	http.HandleFunc("/api/set", setTemperature)
	http.HandleFunc("/api/get", getTemperature)
	log.Fatal(http.ListenAndServe(":80", nil))
}
