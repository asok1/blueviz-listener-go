package main

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/url"
	"os"
)

const (
	serverAddr = "wss://jetstream.atproto.tools/subscribe"
)

var CNX = Connection()

func Connection() *mongo.Client {

	// Use the SetServerAPIOptions() method to set the version of the Stable API on the client
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoUser := os.Getenv("dbUser")
	mongoPassword := os.Getenv("dbPassword")
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb+srv://" + mongoUser + ":" + mongoPassword + "@activity-dev.x8bis.mongodb.net/?retryWrites=true&w=majority&appName=activity-dev").SetServerAPIOptions(serverAPI)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	return client
}

func saveActivity(activityJson string, collection *mongo.Collection) {
	var bsonDoc bson.M
	err := json.Unmarshal([]byte(activityJson), &bsonDoc)
	if err != nil {
		log.Fatal("failed to unmarshal JSON: %s", err)
	}

	_, err = collection.InsertOne(context.TODO(), bsonDoc)
	if err != nil {
		log.Fatal(err)
	}

}
func main() {
	collection := CNX.Database("blueviz-dev").Collection("activity")

	//WEBSOCKET
	wsURL := "wss://jetstream2.us-east.bsky.network/subscribe"

	// Construct query parameters
	u, err := url.Parse(wsURL)
	if err != nil {
		log.Fatalf("failed to parse WebSocket URL: %v", err)
	}
	q := u.Query()
	q.Set("wantedCollections", "app.bsky.feed.post")
	q.Set("wantedCollections", "app.bsky.feed.like")
	q.Set("wantedCollections", "app.bsky.graph.follow")
	u.RawQuery = q.Encode()

	// WebSocket dialer
	dialer := websocket.DefaultDialer

	// Add authorization header
	headers := map[string][]string{}

	// Connect to the WebSocket server
	conn, _, err := dialer.Dial(u.String(), headers)
	if err != nil {
		log.Fatalf("failed to connect to WebSocket: %v", err)
	}
	defer conn.Close()

	log.Println("Connected to WebSocket server")

	// Listen for messages
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("error reading message: %v", err)
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				log.Println("Connection closed normally")
				break
			}
			//log.Println("Reconnecting...")
			//return
		}
		log.Printf("Received message: %s", string(msg))
		saveActivity(string(msg), collection)
	}
}
