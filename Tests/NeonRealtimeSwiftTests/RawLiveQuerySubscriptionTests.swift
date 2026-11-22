import Foundation
@testable import NeonRealtimeSwift
import os
import Testing

@Suite("Logical Neon subscriptions", .timeLimit(.minutes(1)))
struct RawLiveQuerySubscriptionTests {
    @Test
    func keepsTheSameStreamAcrossReconnect() async throws {
        let first = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        let second = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        let calls = OSAllocatedUnfairLock(initialState: 0)
        let delays = OSAllocatedUnfairLock(initialState: [Duration]())
        let subscription = RawLiveQuerySubscription(
            loadQuery: {
                let attempt = calls.withLock { count in
                    count += 1
                    return count
                }
                if attempt == 2 { throw NeonRealtimeSubscriptionError.sealHTTPStatus(503) }
                return makeQuery(capability: "cap-\(attempt)")
            },
            subscribe: { query in query.capability == "cap-1" ? first.stream : second.stream },
            sleepBeforeRetry: { delay in delays.withLock { $0.append(delay) } }
        )
        let original = snapshot(xmax: 10)
        let replacement = snapshot(xmax: 20)
        var updates = await subscription.updates().makeAsyncIterator()
        first.continuation.yield(.snapshot(original))
        #expect(try await updates.next() == .stateChanged(.connecting))
        #expect(try await updates.next() == .event(.snapshot(original)))
        #expect(try await updates.next() == .stateChanged(.live))

        first.continuation.finish(throwing: NeonRealtimeError.server(
            code: "upstream_cancelled",
            message: "Cancelled",
            sqlState: nil,
            retryAfterMilliseconds: 4_500
        ))
        #expect(try await updates.next() == .stateChanged(.reconnecting))
        second.continuation.yield(.snapshot(replacement))
        #expect(try await updates.next() == .event(.snapshot(replacement)))
        #expect(try await updates.next() == .stateChanged(.live))
        #expect(delays.withLock { $0 } == [.milliseconds(4_500), .seconds(2)])
        #expect(calls.withLock { $0 } == 3)

        await subscription.stop()
        #expect(try await updates.next() == .stateChanged(.stopped))
        #expect(try await updates.next() == nil)
        second.continuation.finish()
    }

    @Test
    func retainsQueryIdentityAfterStopAndRejectsAConcurrentConsumer() async throws {
        let frames = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        defer { frames.continuation.finish() }
        let query = OSAllocatedUnfairLock(initialState: makeQuery())
        let opens = OSAllocatedUnfairLock(initialState: 0)
        let subscription = RawLiveQuerySubscription(
            loadQuery: { query.withLock { $0 } },
            subscribe: { _ in
                opens.withLock { $0 += 1 }
                return frames.stream
            }
        )
        let baseline = snapshot(xmax: 10)
        var first = await subscription.updates().makeAsyncIterator()
        frames.continuation.yield(.snapshot(baseline))
        #expect(try await first.next() == .stateChanged(.connecting))
        #expect(try await first.next() == .event(.snapshot(baseline)))
        #expect(try await first.next() == .stateChanged(.live))

        var duplicate = await subscription.updates().makeAsyncIterator()
        await #expect(throws: NeonRealtimeSubscriptionError.alreadyActive) {
            _ = try await duplicate.next()
        }

        await subscription.stop()
        query.withLock { $0 = makeQuery(fingerprint: "another-query") }
        var nextRun = await subscription.updates().makeAsyncIterator()
        #expect(try await nextRun.next() == .stateChanged(.connecting))
        #expect(try await nextRun.next() == .stateChanged(.failed))
        await #expect(throws: NeonRealtimeSubscriptionError.queryFingerprintChanged) {
            _ = try await nextRun.next()
        }
        #expect(await subscription.state == .failed)
        #expect(opens.withLock { $0 } == 1)
    }

    @Test
    func cancellingTheConsumerStopsReconnectBackoff() async throws {
        let sleeping = AsyncStream<Void>.makeStream()
        let cancelled = AsyncStream<Void>.makeStream()
        defer {
            sleeping.continuation.finish()
            cancelled.continuation.finish()
        }
        let loads = OSAllocatedUnfairLock(initialState: 0)
        let subscription = RawLiveQuerySubscription(
            loadQuery: {
                loads.withLock { $0 += 1 }
                throw NeonRealtimeSubscriptionError.sealHTTPStatus(503)
            },
            subscribe: { _ in AsyncThrowingStream { $0.finish() } },
            sleepBeforeRetry: { _ in
                sleeping.continuation.yield(())
                do {
                    try await Task.sleep(for: .seconds(30))
                } catch {
                    cancelled.continuation.yield(())
                    throw error
                }
            }
        )
        let consumer = Task {
            for try await _ in await subscription.updates() {}
        }
        defer { consumer.cancel() }

        var backoff = sleeping.stream.makeAsyncIterator()
        try #require(await backoff.next() != nil)
        consumer.cancel()
        try await consumer.value
        var stopped = cancelled.stream.makeAsyncIterator()
        try #require(await stopped.next() != nil)
        #expect(await subscription.state == .stopped)
        #expect(loads.withLock { $0 } == 1)
    }

    @Test(arguments: [400, 401, 403, 404, 408, 429, 500, 503])
    func distinguishesTransientSealHTTPFailures(status: Int) {
        let delay = RawLiveQuerySubscription.retryDelay(
            after: NeonRealtimeSubscriptionError.sealHTTPStatus(status),
            backoff: .seconds(1)
        )
        #expect(delay == ([408, 429, 500, 503].contains(status) ? .seconds(1) : nil))
    }
}

private func makeQuery(capability: String = "fixture", fingerprint: String = "query") -> SealedLiveQuery {
    SealedLiveQuery(capability: capability, queryFingerprint: fingerprint, expiresAt: 1)
}

private func snapshot(xmax: UInt64) -> NeonSnapshot {
    NeonSnapshot(
        columns: [],
        rows: [],
        visibility: NeonTransactionVisibility(xmin: 1, xmax: xmax, inProgress: []),
        bufferedBatches: []
    )
}
