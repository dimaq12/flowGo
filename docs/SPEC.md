# FlowGo Specification

## 🚀 Purpose

FlowGo is a **type-safe, reactive stream framework for Go**, built with modern concepts from functional programming, algebraic effects, stream processing, and system-level concurrency.

It aims to solve:
- complex data pipelines in Go
- concurrency with cancellation and error handling
- real-time stream control (not just batch-style Rx)

## 🧩 Core Concepts

### 1. `Flow[T]`
A composable, lazy, context-aware pipeline of typed values.
Supports:
- `Map`, `Filter`, `FlatMap`, `Tap`, `ForEach`
- contextual cancellation
- effect propagation and routing

### 2. `Effect`
Flow steps can raise side-effects (errors, retries, logs, timeouts) using:

```go
RaiseEffect(effectType string, data any)
HandleEffect(effectType string, handler func(EffectContext) error)
```

Inspired by algebraic effects and effect handlers.

### 3. `FlowCtx`
FlowCtx carries context (cancel, deadline, values) + trace metadata:

```go
type FlowCtx interface {
    Context() context.Context
    DeadlineExceeded() bool
    WithValue(key, val any) FlowCtx
}
```

### 4. `TypedSwitch`, `UnionMatch`
Flow branching based on type rather than string-based conditionals.

```go
flow.
  SwitchTyped().
    Case(func(x MyTypeA) { ... }).
    Case(func(x MyTypeB) { ... }).
    Default(func(x any) { ... })
```

### 5. `WorkerPool`
Parallelism with backpressure and graceful shutdown.

```go
flow.WithWorkerPool(size int)
```

## 🔌 Integration Interfaces

### `FlowSource[T]`

```go
type FlowSource[T any] interface {
    Start(ctx context.Context, out chan<- T) error
}
```

### `FlowSink[T]`

```go
type FlowSink[T any] interface {
    Write(ctx context.Context, item T) error
}
```

### `FlowDuplex[In, Out]`

Bidirectional streaming interface (gRPC, WebSockets)

```go
type FlowDuplex[In any, Out any] interface {
    Open(ctx context.Context, in <-chan In, out chan<- Out) error
}
```

## 🔄 Extensibility Interfaces

| Interface           | Purpose                          |
|---------------------|----------------------------------|
| `FlowMiddleware`    | Wrap steps with logging/tracing  |
| `FlowHook`          | Lifecycle hooks                  |
| `FlowInspector`     | Describe(), trace, debug info    |
| `FlowConfigurable`  | Live config reload               |
| `FlowCheckpointable`| Save/restore runtime state       |
| `FlowValidator`     | Startup validation               |

## 🧠 Functional Requirements

1. Lazy, context-aware flows
2. Type-safe chaining
3. Centralized effect handling
4. Throttle, debounce, delay
5. Typed branching (SwitchTyped, UnionMatch)
6. Integrated context.Context
7. Retry, timeout, rate control
8. Kafka / Redis / HTTP integration
9. OpenTelemetry-compatible tracing
10. Stateful ops: `Scan`, `Reduce`, `GroupBy`
11. Windowed ops: `Windowed`, `BatchMap`
12. Undo logic: `MapWithUndo`
13. Timed schedulers: `Schedule`, `DelayUntil`
14. Effect plugins: `RegisterEffectHandler`
15. Validation DSL: `.Validate()`
16. Sync ops: `.Join()`, `.Barrier()`
17. Buffered flow: `.WithBuffer(strategy)`
18. Duplex stream: `DuplexFlow[In, Out]`
19. Sharding: `.ShardBy().WorkerPoolPerShard()`
20. Resource-safe steps: `.WithResource().Use()`
21. Live reload config: `.OnConfigChange()`
22. TestKit: `Push()`, `Expect()`, `Simulate()`

## 🏗 Architecture Layers

### 1. Core
- `Flow[T]`
- `Map`, `Filter`, `FlatMap`, `Tap`
- `FlowCtx`, `Effect`
- `FromChannel`, `Run`
- `SwitchTyped`, `UnionMatch`

### 2. flowgo/x/ (optional extensions)
- `.Scan`, `.GroupBy`, `.Windowed`
- `.Schedule`, `.DelayUntil`
- `.MapWithUndo`, `.RetryPolicy`, `.ShardBy`

### 3. labs/ (experimental)
- `.Expect().ThenWithin()` (temporal logic)
- `.DescribeJSON()` (final tagless)
- `.Trace().Explain()` (provenance tracking)
- `.Backward()` (differentiable flows)

## 🔧 Runtime Structure

- Internal executor / scheduler (per-flow)
- Safe shutdown and cancellation
- Buffering and backpressure
- Hook points and metrics
- DSL + introspection (`Describe()`, `Graph()`)

## 🧪 Testing Support

- `TestKit.Push()` — inject data
- `TestKit.Expect()` — assert results
- `TestKit.Log()` — trace internal flow
- Simulate deterministic runs with no goroutines

## 🛣 Roadmap

- [x] Core API
- [x] Filters, map, flatMap, tap, forEach
- [ ] WorkerPool, Retry
- [ ] FlowCtx + Effects
- [ ] Kafka + Redis sources
- [ ] Join, Barrier, Undo
- [ ] OpenTelemetry integration
- [ ] TestKit + DevTools
- [ ] Visual editor (inspired by Temporal UI)