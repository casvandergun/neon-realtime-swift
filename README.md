# NeonRealtimeSwift

A Swift client for the [Neon Realtime v2 protocol](https://neon.com/docs/realtime/sdks/typescript). It streams snapshots, committed change batches, progress, and reset events for a sealed query. It supports iOS 16 and macOS 13 or later and uses Swift tools 6.0.

## Installation

Add the package and link the `NeonRealtimeSwift` product to your target.

```swift
dependencies: [
    .package(url: "https://github.com/casvandergun/neon-realtime-swift.git", from: "0.2.0")
]
```

## Subscribing

A `RealtimeClient` creates a `RawLiveQuerySubscription` for a `SealedLiveQuery`. The subscription keeps streaming across recoverable disconnects. Its loader supplies a fresh capability for the same query on each connection attempt.

```swift
import NeonRealtimeSwift

let client = RealtimeClient(url: realtimeWebSocketURL)
let subscription = client.subscription(loadQuery: api.sealTodos)
for try await update in await subscription.updates() {
    switch update {
    case .stateChanged(let state):
        showStatus(state)
    case .event(let event):
        applyRealtimeEvent(event)
    }
}
```

A replacement snapshot starts a new row ID namespace. Call `await subscription.stop()` or cancel the consuming task to stop sync. The same handle can start again with the same query. For a failed HTTP seal request, throw `NeonRealtimeSubscriptionError.sealHTTPStatus(statusCode)` so the session can classify 408, 429, and 5xx as retryable.

### Single connection attempt

Decode the sealed query from your authenticated backend and connect to the branch's Realtime WebSocket URL. Keep the signing secret on the backend.

```swift
import Foundation
import NeonRealtimeSwift

let query = try JSONDecoder().decode(SealedLiveQuery.self, from: sealedQueryData)
let client = RealtimeClient(url: realtimeWebSocketURL)

for try await event in client.subscribe(to: query) {
    switch event {
    case .snapshot(let snapshot):
        print("Snapshot rows: \(snapshot.rows.count)")
        print("Buffered commits: \(snapshot.bufferedBatches.count)")
    case .changes(let batch):
        print("Committed changes: \(batch.changes.count)")
    case .progress, .resetRequired:
        break
    }
}
```

Each call opens one WebSocket and requires the server to select `neon.realtime.v2` before sending the sealed capability. A snapshot includes column metadata, ordered raw rows, and any commits buffered during snapshot assembly. Install the snapshot and its buffered commits in order before marking the result ready. Apply later change batches atomically, and use transaction IDs and visibility evidence after saving the corresponding events. Row IDs are scoped to the current snapshot. Persist your own stable key from the query result.

The client sends a heartbeat after 20 seconds without a server frame and closes the connection with a timeout if another 10 seconds pass in silence. Cancel the consuming task to close the connection. On timeout, disconnect, or capability expiry, call `subscribe(to:)` again with a fresh sealed query.

For SwiftData persistence and write confirmation, use the [NeonRealtimeSwiftDataCollection adapter](https://github.com/casvandergun/swift-data-collection).

## License

[Apache 2.0](LICENSE).
