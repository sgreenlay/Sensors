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

func withDatabase(op func(context.Context, *mongo.Collection) error) error {
	// Retrieve connection URI
	connecturi := os.Getenv("AZURE_COSMOSDB_CONNECTION_STRING")

	// Create a context to use with the connection
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	// Connect to the DB
	config := options.Client().ApplyURI(connecturi).SetRetryWrites(false).SetDirect(true)
	client, err := mongo.Connect(ctx, config)
	if err != nil {
		return err
	}

	// Ping the DB to confirm the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return err
	}

	collection := client.Database("home").Collection("sensors")

	// Perform DB operation
	err = op(ctx, collection)
	if err != nil {
		return err
	}

	// Close the connection
	err = client.Disconnect(ctx)
	if err != nil {
		return err
	}

	return nil
}

type temperatureReading struct {
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

	var t temperatureReading
	err = json.Unmarshal(body, &t)
	if err != nil {
		panic(err)
	}

	err = withDatabase(func(ctx context.Context, collection *mongo.Collection) error {
		_, err := collection.InsertOne(ctx, t)
		return err
	})
	if err != nil {
		panic(err)
	}
}

func getTemperature(rw http.ResponseWriter, req *http.Request) {
	err := withDatabase(func(ctx context.Context, collection *mongo.Collection) error {
		filter := bson.D{{"room", "test"}}
		opts := options.FindOne().SetSort(bson.D{{"time", -1}})
		var t temperatureReading
		err := collection.FindOne(ctx, filter, opts).Decode(&t)
		if err != nil {
			return err
		}

		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(t)

		return nil
	})
	if err != nil {
		panic(err)
	}
}

func main() {
	http.HandleFunc("/api/set", setTemperature)
	http.HandleFunc("/api/get", getTemperature)
	log.Fatal(http.ListenAndServe(":80", nil))
}
