// examples/simple/main.go
package main

import (
	"context"
	"fmt"

	"github.com/dimaq12/flowgo/flowgo"
)

func main() {
	flow := flowgo.NewFlowFrom(func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 5; i++ {
			out <- i
		}
		return nil
	})

	flow.
		Filter(func(n int) bool { return n%2 == 0 }).
		Map(func(n int) string { return fmt.Sprintf("Even: %d", n) }).
		Tap(func(s string) { fmt.Println("Tapped:", s) }).
		ForEach(func(s string) { fmt.Println("Final:", s) })
}
