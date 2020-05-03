package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"log"
	"time"
)

type temperatureReading struct {
	Room        string
	Time        string
	Temperature float64
	Humidity    float64
}

func main() {
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

	// Change the timeout of our context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)

	// Create an example reading
	reading := temperatureReading{
		Room:        "test",
		Time:        "2020-05-02T20:02:24.599Z",
		Temperature: 19.0,
		Humidity:    50.0,
	}
	res, err := collection.InsertOne(ctx, reading)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserted a single document from a struct: ", res.InsertedID)

	// Get the latest reading
	filter := bson.D{{"room", "test"}}
	opts := options.FindOne().SetSort(bson.D{{"time", -1}})
	var t temperatureReading
	err = collection.FindOne(ctx, filter, opts).Decode(&t)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(
		"Time:", t.Time,
		"Temperature:", t.Temperature)

	// Close the connection
	err = client.Disconnect(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
