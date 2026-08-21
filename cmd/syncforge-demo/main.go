// Command syncforge-demo narrates SyncForge's headline scenario in plain
// English:
//
//	             ┌── Device A
//	             │
//	Server ──────┼── Device B
//	             │
//	             └── Device C
//
// Three devices go offline, independently edit the same record, and
// reconnect in a scrambled order — converging deterministically on the
// same value regardless. See test/integration for the same scenario
// proven automatically across every possible reconnect order.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stan-ley-tech/SyncForge/internal/server"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/client"
)

type profile struct {
	Name string `json:"name"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("syncforge-demo: %v", err)
	}
}

func run() error {
	fmt.Println("SyncForge demo: three devices, one record, an offline conflict.")
	fmt.Println()

	tmpDir, err := os.MkdirTemp("", "syncforge-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	serverURL, stop, err := startServer(filepath.Join(tmpDir, "server.db"))
	if err != nil {
		return err
	}
	defer stop()
	fmt.Printf("Server listening at %s\n\n", serverURL)

	ctx := context.Background()

	alice, err := openDevice(ctx, tmpDir, serverURL, "alice", "Alice's Laptop")
	if err != nil {
		return err
	}
	defer alice.Close()
	bob, err := openDevice(ctx, tmpDir, serverURL, "bob", "Bob's Phone")
	if err != nil {
		return err
	}
	defer bob.Close()
	carol, err := openDevice(ctx, tmpDir, serverURL, "carol", "Carol's Tablet")
	if err != nil {
		return err
	}
	defer carol.Close()

	names := map[*client.Client]string{alice: "Alice", bob: "Bob", carol: "Carol"}

	fmt.Println("Step 1: Alice creates a shared record and everyone syncs to establish a common starting point.")
	if err := alice.Put(ctx, "profiles", "user-1", profile{Name: "initial"}); err != nil {
		return err
	}
	for _, d := range []*client.Client{alice, bob, carol} {
		if _, err := d.Sync(ctx); err != nil {
			return err
		}
	}
	printState(ctx, "  ", alice, bob, carol)
	fmt.Println()

	fmt.Println("Step 2: all three devices go offline and independently edit the SAME record.")
	if err := alice.Put(ctx, "profiles", "user-1", profile{Name: "Alice's edit"}); err != nil {
		return err
	}
	fmt.Println("  Alice (offline):  \"Alice's edit\"")
	time.Sleep(5 * time.Millisecond)
	if err := bob.Put(ctx, "profiles", "user-1", profile{Name: "Bob's edit"}); err != nil {
		return err
	}
	fmt.Println("  Bob   (offline):  \"Bob's edit\"")
	time.Sleep(5 * time.Millisecond)
	if err := carol.Put(ctx, "profiles", "user-1", profile{Name: "Carol's edit"}); err != nil {
		return err
	}
	fmt.Println("  Carol (offline):  \"Carol's edit\"")
	fmt.Println()

	fmt.Println("Step 3: reconnect in a deliberately scrambled order — Bob, then Carol, then Alice — twice, so everyone catches up.")
	order := []*client.Client{bob, carol, alice}
	for round := 1; round <= 2; round++ {
		for _, d := range order {
			result, err := d.Sync(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("  round %d: %-5s synced (pushed=%d pulled=%d conflicts=%d)\n",
				round, names[d], result.Pushed, result.Pulled, len(result.Conflicts))
			for _, conf := range result.Conflicts {
				fmt.Printf("           -> conflict on %s/%s resolved: winner=%s loser=%s\n",
					conf.Collection, conf.RecordID, shortID(conf.WinningOpID), shortID(conf.LosingOpID))
			}
		}
	}
	fmt.Println()

	fmt.Println("Step 4: final state.")
	printState(ctx, "  ", alice, bob, carol)

	va, err := recordName(ctx, alice)
	if err != nil {
		return err
	}
	vb, err := recordName(ctx, bob)
	if err != nil {
		return err
	}
	vc, err := recordName(ctx, carol)
	if err != nil {
		return err
	}
	if va != vb || vb != vc {
		return fmt.Errorf("devices did not converge: alice=%q bob=%q carol=%q", va, vb, vc)
	}
	fmt.Printf("\nAll three devices converged deterministically on: %q\n", va)
	return nil
}

// shortID trims a UUID op id down to its first segment for readable
// narration output; the full id is only needed for programmatic lookups.
func shortID(id string) string {
	if i := strings.IndexByte(id, '-'); i > 0 {
		return id[:i]
	}
	return id
}

func printState(ctx context.Context, prefix string, alice, bob, carol *client.Client) {
	va, _ := recordName(ctx, alice)
	vb, _ := recordName(ctx, bob)
	vc, _ := recordName(ctx, carol)
	fmt.Printf("%sAlice=%q Bob=%q Carol=%q\n", prefix, va, vb, vc)
}

func recordName(ctx context.Context, c *client.Client) (string, error) {
	rec, found, err := c.Get(ctx, "profiles", "user-1")
	if err != nil || !found {
		return "", err
	}
	var p profile
	if err := json.Unmarshal(rec.Value, &p); err != nil {
		return "", err
	}
	return p.Name, nil
}

func startServer(dbPath string) (url string, stop func(), err error) {
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return "", nil, err
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		return "", nil, err
	}

	httpSrv := &http.Server{Handler: server.New(db).Handler()}
	go httpSrv.Serve(ln)

	stop = func() {
		httpSrv.Close()
		db.Close()
	}
	return "http://" + ln.Addr().String(), stop, nil
}

func openDevice(ctx context.Context, dir, serverURL, id, displayName string) (*client.Client, error) {
	c, err := client.Open(filepath.Join(dir, id+".db"), serverURL)
	if err != nil {
		return nil, err
	}
	if err := c.Register(ctx, displayName); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
