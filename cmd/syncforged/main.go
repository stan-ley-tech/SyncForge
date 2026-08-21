// Command syncforged runs the SyncForge sync server: device registration
// plus the push/pull REST API, backed by a SQLite database file.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/stan-ley-tech/SyncForge/internal/server"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "syncforged.db", "path to the SQLite database file")
	flag.Parse()

	db, err := sqlite.Open(*dbPath)
	if err != nil {
		log.Fatalf("syncforged: opening database %s: %v", *dbPath, err)
	}
	defer db.Close()

	srv := server.New(db)
	log.Printf("syncforged: listening on %s (db=%s)", *addr, *dbPath)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatalf("syncforged: %v", err)
	}
}
