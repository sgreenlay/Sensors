package main

import (
	"context"
	"encoding/json"
	"errors"
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
	connectURI, foundURI := os.LookupEnv("AZURE_COSMOSDB_CONNECTION_STRING")
	if (!foundURI) {
		return errors.New("Must set AZURE_COSMOSDB_CONNECTION_STRING")
	}

	// Create a context to use with the connection
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	// Connect to the DB
	config := options.Client().ApplyURI(connectURI).SetRetryWrites(false).SetDirect(true)
	client, err := mongo.Connect(ctx, config)
	if err != nil {
		return err
	}

	// Ping the DB to confirm the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return err
	}

	// Retrieve database collection
	databaseName, foundDatabaseName := os.LookupEnv("SENSORS_DATABASE")
	if (!foundDatabaseName) {
		databaseName = "home"
	}
	collectionName, foundCollectionName := os.LookupEnv("SENSORS_COLLECTION")
	if (!foundCollectionName) {
		collectionName = "sensors"
	}
	collection := client.Database(databaseName).Collection(collectionName)

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
	rooms, ok := req.URL.Query()["room"]
	if !ok || len(rooms[0]) < 1 {
        log.Println("Url Param 'key' is missing")
        return
    }
	
	err := withDatabase(func(ctx context.Context, collection *mongo.Collection) error {
		filter := bson.D{{"room", rooms[0]}}
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
