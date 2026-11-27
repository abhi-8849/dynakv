// Command dynakv-cli is the operator CLI: read and write keys and inspect the ring of a dynakv cluster.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/abhi-8849/dynakv/client"
)

const callTimeout = 10 * time.Second

type options struct {
	seeds string
	node  string
	r, w  int
	ctx   string
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dynakv-cli:", err)
		os.Exit(1)
	}
}

// parse reads flags that may appear before, between or after the positional arguments.
func parse(args []string) (options, []string, error) {
	var o options
	fs := flag.NewFlagSet("dynakv-cli", flag.ContinueOnError)
	fs.StringVar(&o.seeds, "seeds", "localhost:7000", "comma-separated seed node addresses")
	fs.StringVar(&o.node, "node", "", "send every request to this one node, skipping ring routing")
	fs.IntVar(&o.r, "r", 0, "read quorum for get; 0 uses the node default")
	fs.IntVar(&o.w, "w", 0, "write quorum for put and delete; 0 uses the node default")
	fs.StringVar(&o.ctx, "ctx", "", "base64 context from a previous get, for put and delete")
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return o, nil, err
		}
		if fs.NArg() == 0 {
			return o, pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func (o options) writeOpts() ([]client.WriteOpt, error) {
	opts := []client.WriteOpt{client.WithW(o.w)}
	if o.ctx != "" {
		vctx, err := base64.StdEncoding.DecodeString(o.ctx)
		if err != nil {
			return nil, fmt.Errorf("decode -ctx: %w", err)
		}
		opts = append(opts, client.WithContext(vctx))
	}
	return opts, nil
}

// dial connects through the seeds, or to the single -node without ring routing.
func (o options) dial(ctx context.Context) (*client.Client, error) {
	if o.node != "" {
		return client.Dial(ctx, []string{o.node}, client.WithoutRouting())
	}
	return client.Dial(ctx, strings.Split(o.seeds, ","))
}

func run(args []string, stdin io.Reader, out io.Writer) error {
	o, pos, err := parse(args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New("usage: dynakv-cli [-seeds host:port] [-node host:port] [-r N] [-w N] [-ctx base64] put <key> <value|-> | get <key> | delete <key> | owners <key> | status")
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	switch cmd, rest := pos[0], pos[1:]; {
	case cmd == "put" && len(rest) == 2:
		return put(ctx, c, o, []byte(rest[0]), rest[1], stdin, out)
	case cmd == "get" && len(rest) == 1:
		return get(ctx, c, o, []byte(rest[0]), out)
	case cmd == "delete" && len(rest) == 1:
		return del(ctx, c, o, []byte(rest[0]), out)
	case cmd == "owners" && len(rest) == 1:
		return owners(ctx, c, []byte(rest[0]), out)
	case cmd == "status" && len(rest) == 0:
		return status(ctx, c, out)
	}
	return fmt.Errorf("unknown or incomplete command %q", strings.Join(pos, " "))
}

// put stores value; a value of "-" is read from stdin.
func put(ctx context.Context, c *client.Client, o options, key []byte, value string, stdin io.Reader, out io.Writer) error {
	data := []byte(value)
	if value == "-" {
		var err error
		if data, err = io.ReadAll(stdin); err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
	}
	opts, err := o.writeOpts()
	if err != nil {
		return err
	}
	if err := c.Put(ctx, key, data, opts...); err != nil {
		return err
	}
	fmt.Fprintln(out, "OK")
	return nil
}

// get prints each sibling on its own line, then the context to pass back on a write.
func get(ctx context.Context, c *client.Client, o options, key []byte, out io.Writer) error {
	values, vctx, err := c.Get(ctx, key, client.WithR(o.r))
	if err != nil {
		return err
	}
	for _, v := range values {
		fmt.Fprintf(out, "%s\n", v)
	}
	fmt.Fprintf(out, "context: %s\n", base64.StdEncoding.EncodeToString(vctx))
	return nil
}

func del(ctx context.Context, c *client.Client, o options, key []byte, out io.Writer) error {
	opts, err := o.writeOpts()
	if err != nil {
		return err
	}
	if err := c.Delete(ctx, key, opts...); err != nil {
		return err
	}
	fmt.Fprintln(out, "OK")
	return nil
}

// status prints every known node and whether it is alive.
func status(ctx context.Context, c *client.Client, out io.Writer) error {
	members, err := c.Status(ctx)
	if err != nil {
		return err
	}
	return printMembers(out, members)
}

// owners prints the key's preference list, the first live node being its coordinator.
func owners(ctx context.Context, c *client.Client, key []byte, out io.Writer) error {
	members, err := c.Owners(ctx, key)
	if err != nil {
		return err
	}
	return printMembers(out, members)
}

func printMembers(out io.Writer, members []client.Member) error {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tADDR\tZONE\tSTATE")
	for _, m := range members {
		state := "alive"
		if !m.Alive {
			state = "dead"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", m.ID, m.Addr, m.Zone, state)
	}
	return w.Flush()
}
