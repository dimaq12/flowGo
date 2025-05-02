package flowgo

import (
	"context"
	"sync"
)

// Flow represents a typed reactive stream of values
// with composition operators and managed execution

type Flow[T any] struct {
	source func(ctx context.Context, out chan<- T) error
}

// NewFlowFrom creates a Flow from a generator function
func NewFlowFrom[T any](src func(ctx context.Context, out chan<- T) error) Flow[T] {
	return Flow[T]{source: src}
}

// Map applies a transformation function to each element in the stream
func (f Flow[T]) Map[R any](fn func(T) R) Flow[R] {
	return NewFlowFrom(func(ctx context.Context, out chan<- R) error {
		in := make(chan T)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range in {
				select {
				case <-ctx.Done():
					return
				case out <- fn(item):
				}
			}
		}()
		err := f.source(ctx, in)
		close(in)
		wg.Wait()
		return err
	})
}

// Filter passes through only items that satisfy the predicate
func (f Flow[T]) Filter(pred func(T) bool) Flow[T] {
	return NewFlowFrom(func(ctx context.Context, out chan<- T) error {
		in := make(chan T)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range in {
				if pred(item) {
					select {
					case <-ctx.Done():
						return
					case out <- item:
					}
				}
			}
		}()
		err := f.source(ctx, in)
		close(in)
		wg.Wait()
		return err
	})
}

// Tap performs a side effect without modifying the item
func (f Flow[T]) Tap(fn func(T)) Flow[T] {
	return f.Map(func(t T) T {
		fn(t)
		return t
	})
}

// FlatMap transforms each item into a sub-flow and flattens the result
func (f Flow[T]) FlatMap[R any](fn func(T) Flow[R]) Flow[R] {
	return NewFlowFrom(func(ctx context.Context, out chan<- R) error {
		in := make(chan T)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			var subWg sync.WaitGroup
			for item := range in {
				subFlow := fn(item)
				subCh := make(chan R)
				subWg.Add(1)
				go func() {
					defer subWg.Done()
					for subItem := range subCh {
						select {
						case <-ctx.Done():
							return
						case out <- subItem:
						}
					}
				}()
				_ = subFlow.source(ctx, subCh)
				close(subCh)
			}
			subWg.Wait()
		}()
		err := f.source(ctx, in)
		close(in)
		wg.Wait()
		return err
	})
}

// ForEach consumes the flow and applies a side effect
func (f Flow[T]) ForEach(fn func(T)) error {
	ctx := context.Background()
	ch := make(chan T)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for item := range ch {
			fn(item)
		}
	}()
	err := f.source(ctx, ch)
	close(ch)
	wg.Wait()
	return err
}