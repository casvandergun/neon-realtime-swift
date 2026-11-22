import Foundation

public enum NeonRealtimeSubscriptionError: Error, Sendable, Equatable {
    case alreadyActive
    case queryFingerprintChanged
    /// The HTTP status of a failed capability request.
    case sealHTTPStatus(Int)
}

/// A logical raw subscription that survives recoverable connection failures.
public actor RawLiveQuerySubscription {
    public enum State: Sendable, Equatable {
        case idle
        case connecting
        /// A complete baseline has been emitted for this connection.
        case live
        case reconnecting
        case failed
        case stopped
    }

    public enum Update: Sendable, Equatable {
        case stateChanged(State)
        case event(NeonRealtimeEvent)
    }

    public private(set) var state: State = .idle

    private struct Run {
        let id: UUID
        let continuation: AsyncThrowingStream<Update, Error>.Continuation
        let reader: Task<Void, Never>
    }

    private let loadQuery: @Sendable () async throws -> SealedLiveQuery
    private let subscribe: @Sendable (SealedLiveQuery) -> AsyncThrowingStream<NeonRealtimeEvent, Error>
    private let sleepBeforeRetry: @Sendable (Duration) async throws -> Void
    private var run: Run?
    private var queryFingerprint: String?

    init(
        loadQuery: @escaping @Sendable () async throws -> SealedLiveQuery,
        subscribe: @escaping @Sendable (SealedLiveQuery) -> AsyncThrowingStream<NeonRealtimeEvent, Error>,
        sleepBeforeRetry: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) }
    ) {
        self.loadQuery = loadQuery
        self.subscribe = subscribe
        self.sleepBeforeRetry = sleepBeforeRetry
    }

    /// Starts a single consumer stream. Lifecycle transitions and protocol events
    /// arrive in order. Recoverable failures keep the stream open.
    public func updates() -> AsyncThrowingStream<Update, Error> {
        guard run == nil else {
            return AsyncThrowingStream { $0.finish(throwing: NeonRealtimeSubscriptionError.alreadyActive) }
        }
        let output = AsyncThrowingStream<Update, Error>.makeStream()
        let id = UUID()
        output.continuation.onTermination = { [weak self] termination in
            if case .cancelled = termination {
                Task { await self?.stop(runID: id) }
            }
        }
        let reader = Task { await read() }
        run = Run(id: id, continuation: output.continuation, reader: reader)
        transition(to: .connecting)
        return output.stream
    }

    /// Stops the active stream. A later call to updates starts the same query again.
    public func stop() {
        guard let run else { return }
        run.reader.cancel()
        finish(.stopped)
    }

    private func stop(runID: UUID) {
        guard run?.id == runID else { return }
        stop()
    }

    private func read() async {
        var backoff = Duration.seconds(1)
        while !Task.isCancelled {
            do {
                let query = try await loadQuery()
                try Task.checkCancellation()
                guard queryFingerprint == nil || queryFingerprint == query.queryFingerprint else {
                    throw NeonRealtimeSubscriptionError.queryFingerprintChanged
                }
                queryFingerprint = query.queryFingerprint
                for try await event in subscribe(query) {
                    try Task.checkCancellation()
                    run?.continuation.yield(.event(event))
                    if case .snapshot = event {
                        backoff = .seconds(1)
                        transition(to: .live)
                    } else if case .resetRequired = event {
                        transition(to: .connecting)
                    }
                }
                throw URLError(.networkConnectionLost)
            } catch {
                guard !Task.isCancelled else { return }
                guard let delay = Self.retryDelay(after: error, backoff: backoff) else {
                    finish(.failed, throwing: error)
                    return
                }
                transition(to: .reconnecting)
                do {
                    try await sleepBeforeRetry(delay)
                } catch {
                    if !Task.isCancelled { finish(.failed, throwing: error) }
                    return
                }
                backoff = min(backoff * 2, .seconds(15))
            }
        }
    }

    private func transition(to state: State) {
        guard self.state != state else { return }
        self.state = state
        run?.continuation.yield(.stateChanged(state))
    }

    private func finish(_ state: State, throwing error: Error? = nil) {
        transition(to: state)
        run?.continuation.finish(throwing: error)
        run = nil
    }

    static func retryDelay(after error: Error, backoff: Duration) -> Duration? {
        switch error {
        case NeonRealtimeSubscriptionError.sealHTTPStatus(let status):
            switch status {
            case 408, 429, 500...599: return backoff
            default: return nil
            }
        case NeonRealtimeError.server(let code, _, _, let retryAfterMilliseconds):
            switch code {
            case "authorization_expired", "backend_overloaded", "backend_session_lost",
                 "backend_unavailable", "baseline_sync_failed", "fence_failed", "heartbeat_timeout",
                 "internal_error", "resource_exhausted", "service_unavailable", "slow_consumer",
                 "upstream_cancelled":
                if let retryAfterMilliseconds {
                    return max(backoff, .milliseconds(Int64(retryAfterMilliseconds)))
                }
                return backoff
            default:
                return nil
            }
        case let error as URLError:
            switch error.code {
            case .timedOut, .cannotFindHost, .cannotConnectToHost, .dnsLookupFailed,
                 .networkConnectionLost, .notConnectedToInternet: return backoff
            default: return nil
            }
        default:
            return nil
        }
    }
}
