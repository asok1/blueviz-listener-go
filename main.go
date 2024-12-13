package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"log"
	"log/slog"
	"os"
	"time"

	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/models"
)

const (
	serverAddr = "wss://jetstream.atproto.tools/subscribe"
)

func main() {

	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})))
	logger := slog.Default()

	config := client.DefaultClientConfig()
	config.WebsocketURL = serverAddr
	config.Compress = true

	h := &handler{
		seenSeqs: make(map[int64]struct{}),
	}

	scheduler := parallel.NewScheduler(5, "jetstream_localdev", logger, h.HandleEvent)

	c, err := client.NewClient(config, logger, scheduler)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	cursor := time.Now().Add(5 * -time.Minute).UnixMicro()

	// Every 5 seconds print the events read and bytes read and average event size
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				eventsRead := c.EventsRead.Load()
				bytesRead := c.BytesRead.Load()
				avgEventSize := bytesRead / eventsRead
				logger.Info("stats", "events_read", eventsRead, "bytes_read", bytesRead, "avg_event_size", avgEventSize)
			}
		}
	}()

	if err := c.ConnectAndRead(ctx, &cursor); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	slog.Info("shutdown")
}

type handler struct {
	seenSeqs  map[int64]struct{}
	highwater int64
}

func (h *handler) HandleEvent(ctx context.Context, event *models.Event) error {
	// Unmarshal the record if there is one
	if event.Commit != nil && (event.Commit.Operation == models.CommitOperationCreate || event.Commit.Operation == models.CommitOperationUpdate) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			} else {
				log.Print(post)
			}

			err := handleSavePost(os.Getenv("dbPassword"), post.CreatedAt, event.Did, event.Kind, event.Commit.Collection, event.Commit.Operation, event.Commit.CID, event.Commit.Record)
			if err != nil {
				return fmt.Errorf("failed to save post: %s", err)
			}
			fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Text)
			//case "app.bsky.feed.like":
			//	var like apibsky.FeedLike
			//	if err := json.Unmarshal(event.Commit.Record, &like); err != nil {
			//		return fmt.Errorf("failed to unmarshal like: %w", err)
			//	}
			//	fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, like.Subject)
			//case "app.bsky.feed.repost":
			//	var post apibsky.FeedRepost
			//	if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
			//		return fmt.Errorf("failed to unmarshal post: %w", err)
			//	}
			//	fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Subject)
			//case "app.bsky.graph.follow":
			//	var post apibsky.GraphFollow
			//	if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
			//		return fmt.Errorf("failed to unmarshal post: %w", err)
			//	}
			//	fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Subject)
			//case "app.bsky.actor.profile":
			//	var post apibsky.ActorProfile
			//	if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
			//		return fmt.Errorf("failed to unmarshal post: %w", err)
			//	}
			//	fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Description)
			//case "app.bsky.graph.block":
			//	var post apibsky.GraphBlock
			//	if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
			//		return fmt.Errorf("failed to unmarshal post: %w", err)
			//	}
			//	fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Subject)
			//case "app.bsky.graph.listitem":
			//	var post apibsky.GraphListitem
			//	if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
			//		return fmt.Errorf("failed to unmarshal post: %w", err)
			//	}
			//	fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Subject)
			//default:
			//	log.Print("failed to unmarshal post: %w of type %w", event, event.Commit.Collection)
		}

	}

	return nil
}
