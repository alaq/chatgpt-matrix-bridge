// source-check checks the real collector boundary without any Matrix side effects.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/alaq/chatgpt-matrix-bridge/internal/source"
	"os"
	"time"
)

func main() {
	var b source.Backend
	flag.StringVar(&b.Python, "python", "python3", "Python executable")
	flag.StringVar(&b.Directory, "backend", "", "Absolute path to backend checkout")
	flag.StringVar(&b.Archive, "archive", "", "Absolute path to private archive")
	flag.StringVar(&b.Descriptor, "descriptor", "", "Absolute path to DEV descriptor")
	refresh := flag.Bool("refresh", false, "Run incremental collection before reading")
	sinceArg := flag.String("since", "", "RFC3339 activation boundary; default now")
	flag.Parse()
	since := time.Now()
	var err error
	if *sinceArg != "" {
		since, err = time.Parse(time.RFC3339, *sinceArg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid since timestamp")
			os.Exit(1)
		}
	}
	ctx := context.Background()
	if *refresh {
		if err = b.Refresh(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	snapshot, err := b.Read(ctx, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	chats, err := source.Select(snapshot, snapshot.AccountKey, float64(since.UnixMilli())/1000)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	messages := 0
	for _, c := range chats {
		messages += len(c.Messages)
	}
	// Print counts, never conversation text, titles, account identifiers or credentials.
	json.NewEncoder(os.Stdout).Encode(map[string]any{"source": snapshot.Source, "conversations_available": len(snapshot.Conversations), "eligible_rooms": len(chats), "eligible_messages": messages, "coverage": snapshot.Coverage, "matrix_writes": 0})
}
