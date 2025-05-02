package flowgo

import (
	"context"
	"testing"
)

func TestMap(t *testing.T) {
	src := NewFlowFrom(func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 3; i++ {
			out <- i
		}
		return nil
	})

	var results []string
	err := src.
		Map(func(i int) string { return "N=" + string('0'+i) }).
		ForEach(func(s string) {
			results = append(results, s)
		})

	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 items, got %d", len(results))
	}
}

func TestFilter(t *testing.T) {
	src := NewFlowFrom(func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 5; i++ {
			out <- i
		}
		return nil
	})

	var results []int
	err := src.
		Filter(func(i int) bool { return i%2 == 0 }).
		ForEach(func(i int) {
			results = append(results, i)
		})

	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 items, got %d", len(results))
	}
}

func TestFlatMap(t *testing.T) {
	src := NewFlowFrom(func(ctx context.Context, out chan<- int) error {
		out <- 1
		out <- 2
		return nil
	})

	var results []int
	err := src.
		FlatMap(func(i int) Flow[int] {
			return NewFlowFrom(func(ctx context.Context, out chan<- int) error {
				out <- i
				out <- i * 10
				return nil
			})
		}).
		ForEach(func(i int) {
			results = append(results, i)
		})

	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 items, got %d", len(results))
	}
}

func TestTap(t *testing.T) {
	var taps []int
	src := NewFlowFrom(func(ctx context.Context, out chan<- int) error {
		out <- 42
		return nil
	})

	err := src.
		Tap(func(i int) {
			taps = append(taps, i)
		}).
		ForEach(func(i int) {})

	if err != nil {
		t.Fatal(err)
	}
	if len(taps) != 1 || taps[0] != 42 {
		t.Errorf("tap failed, got %+v", taps)
	}
}
