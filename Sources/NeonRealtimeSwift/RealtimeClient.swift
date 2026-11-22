import Foundation

private let neonRealtimeSubprotocol = "neon.realtime.v2"

public struct RealtimeClient: Sendable {
    public let url: URL
    private let makeSocket: @Sendable (URL) -> any NeonWebSocket
    private let heartbeatIdle: Duration
    private let heartbeatTimeout: Duration

    public init(url: URL, session: URLSession = .shared) {
        self.init(url: url) { URLSessionNeonWebSocket(url: $0, session: session) }
    }

    init(
        url: URL,
        heartbeatIdle: Duration = .seconds(20),
        heartbeatTimeout: Duration = .seconds(10),
        makeSocket: @escaping @Sendable (URL) -> any NeonWebSocket
    ) {
        self.url = url
        self.heartbeatIdle = heartbeatIdle
        self.heartbeatTimeout = heartbeatTimeout
        self.makeSocket = makeSocket
    }

    /// Creates a logical subscription. The loader supplies fresh capabilities for
    /// the same query on connection attempts and before expiry.
    public func subscription(
        loadQuery: @escaping @Sendable () async throws -> SealedLiveQuery
    ) -> RawLiveQuerySubscription {
        RawLiveQuerySubscription(loadQuery: loadQuery, connect: { query, renewal in
            subscribe(to: query, renewWith: renewal)
        })
    }

    /// Opens a connection for one sealed query and emits updates in application order.
    /// Cancel the consuming task to close the connection. On expiry or disconnect,
    /// subscribe again with a fresh sealed query to obtain a replacement snapshot.
    public func subscribe(to query: SealedLiveQuery) -> AsyncThrowingStream<NeonRealtimeEvent, Error> {
        subscribe(to: query, renewWith: nil)
    }

    private func subscribe(
        to query: SealedLiveQuery,
        renewWith loadRenewal: LoadRenewedQuery?
    ) -> AsyncThrowingStream<NeonRealtimeEvent, Error> {
        AsyncThrowingStream { continuation in
            guard isSupportedEndpoint else {
                continuation.finish(throwing: NeonRealtimeError.invalidEndpoint)
                return
            }
            guard query.hasValidCapability else {
                continuation.finish(throwing: NeonRealtimeError.invalidSealedQuery)
                return
            }

            let socket = makeSocket(url)
            let task = Task {
                await runSubscription(query, renewWith: loadRenewal, on: socket, into: continuation)
            }
            continuation.onTermination = { _ in
                task.cancel()
                socket.close()
            }
        }
    }

    private var isSupportedEndpoint: Bool {
        guard let host = url.host, !host.isEmpty else { return false }
        switch url.scheme?.lowercased() {
        case "wss":
            return true
        case "ws":
            return ["localhost", "127.0.0.1", "::1", "[::1]"].contains(host.lowercased())
        default:
            return false
        }
    }

    private func runSubscription(
        _ query: SealedLiveQuery,
        renewWith loadRenewal: LoadRenewedQuery?,
        on socket: any NeonWebSocket,
        into continuation: AsyncThrowingStream<NeonRealtimeEvent, Error>.Continuation
    ) async {
        let activity = NeonSocketActivity()
        let outbound = AsyncStream<NeonClientMessage>.makeStream(bufferingPolicy: .bufferingOldest(2))
        let renewal = loadRenewal.map { _ in NeonConnectionRenewal() }
        await withThrowingTaskGroup(of: Void.self) { group in
            defer {
                group.cancelAll()
                outbound.continuation.finish()
                // Close before the group waits for a blocked receive or send.
                socket.close()
            }
            group.addTask {
                try await Self.receive(query.capability, from: socket, into: continuation, activity: activity, renewal: renewal)
            }
            group.addTask {
                let encoder = JSONEncoder()
                encoder.keyEncodingStrategy = .convertToSnakeCase
                for await message in outbound.stream {
                    try Task.checkCancellation()
                    try await Self.send(message, through: socket, encoder: encoder)
                }
            }
            group.addTask {
                try await activity.monitor(idle: heartbeatIdle, timeout: heartbeatTimeout, outbound: outbound.continuation)
            }
            if let renewal, let loadRenewal {
                group.addTask {
                    try await renewal.run(
                        startingWith: query,
                        load: loadRenewal,
                        outbound: outbound.continuation,
                        acknowledgementTimeout: heartbeatTimeout
                    )
                }
            }

            // Report the connection's result before waiting for its tasks to
            // stop. A cancelled application loader can still be unwinding.
            do {
                _ = try await group.next()
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
    }

    private static func receive(
        _ capability: String,
        from socket: any NeonWebSocket,
        into continuation: AsyncThrowingStream<NeonRealtimeEvent, Error>.Continuation,
        activity: NeonSocketActivity,
        renewal: NeonConnectionRenewal?
    ) async throws {
        let negotiatedSubprotocol = try await socket.negotiatedSubprotocol()
        try Task.checkCancellation()
        guard negotiatedSubprotocol == neonRealtimeSubprotocol else {
            throw NeonRealtimeError.unexpectedSubprotocol(actual: negotiatedSubprotocol)
        }

        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        var ready = false
        var state = NeonSubscriptionState()

        while true {
            try Task.checkCancellation()
            let frame = try await socket.receive()
            try Task.checkCancellation()
            let message: NeonServerMessage
            do {
                message = try decoder.decode(NeonServerMessage.self, from: Data(frame.utf8))
            } catch is DecodingError {
                throw NeonRealtimeError.protocolViolation("Invalid Realtime v2 frame")
            }

            await activity.recordFrame()
            switch message {
            case .ping(let token):
                try await send(.pong(token: token), through: socket, encoder: encoder)
            case .ready where !ready:
                try await send(.clientInfo, through: socket, encoder: encoder)
                try await send(.subscribe(authorization: capability), through: socket, encoder: encoder)
                ready = true
                await activity.enableProbes()
            case .failure(let failure) where !ready && failure.requestId == nil && failure.liveId == nil:
                throw failure.error
            case .renewed(let liveID) where ready && renewal != nil:
                try await renewal?.acknowledge(liveID)
            default:
                guard ready else {
                    throw NeonRealtimeError.protocolViolation("Expected Realtime ready")
                }
                let events = try state.apply(message)
                if case .subscribed(let admission) = message {
                    await renewal?.admit(admission.liveId)
                }
                for event in events {
                    if case .terminated = continuation.yield(event) {
                        return
                    }
                }
            }
        }
    }

    private static func send(
        _ message: NeonClientMessage,
        through socket: any NeonWebSocket,
        encoder: JSONEncoder
    ) async throws {
        let payload = try encoder.encode(message)
        try await socket.send(String(decoding: payload, as: UTF8.self))
    }
}

private struct NeonClientMessage: Encodable {
    let type: String
    var metadata: String?
    var requestId: String?
    var liveId: String?
    var authorization: String?
    var token: String?

    static let clientInfo = Self(type: "client_info", metadata: #"{"sdk":"swift"}"#)
    static let heartbeatPing = Self(type: "ping", token: "swift-heartbeat")

    static func subscribe(authorization: String) -> Self {
        Self(type: "subscribe", requestId: NeonSubscriptionState.requestID, authorization: authorization)
    }

    static func renew(liveID: NeonWireID, authorization: String) -> Self {
        Self(type: "renew", liveId: String(liveID.value), authorization: authorization)
    }

    static func pong(token: String) -> Self {
        Self(type: "pong", token: token)
    }
}

private actor NeonConnectionRenewal {
    private let admission = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingOldest(1))
    private var liveID: NeonWireID?
    private var pendingAcknowledgement: AsyncThrowingStream<Void, Error>.Continuation?

    func admit(_ id: NeonWireID) {
        liveID = id
        admission.continuation.yield(())
        admission.continuation.finish()
    }

    func acknowledge(_ id: NeonWireID) throws {
        guard liveID == id, let pendingAcknowledgement else {
            throw NeonRealtimeError.protocolViolation("Unexpected renewal acknowledgement")
        }
        self.pendingAcknowledgement = nil
        pendingAcknowledgement.yield(())
        pendingAcknowledgement.finish()
    }

    func run(
        startingWith query: SealedLiveQuery,
        load: LoadRenewedQuery,
        outbound: AsyncStream<NeonClientMessage>.Continuation,
        acknowledgementTimeout: Duration
    ) async throws {
        var admissions = admission.stream.makeAsyncIterator()
        guard await admissions.next() != nil, let liveID else { throw CancellationError() }

        var current = query
        while true {
            let replacement = try await load(current)
            try Task.checkCancellation()
            let acknowledgement = AsyncThrowingStream<Void, Error>.makeStream()
            pendingAcknowledgement = acknowledgement.continuation
            let deadline = Task {
                do {
                    try await Task.sleep(for: acknowledgementTimeout)
                    acknowledgement.continuation.finish(throwing: URLError(.timedOut))
                } catch is CancellationError {}
            }
            defer { deadline.cancel() }
            guard case .enqueued = outbound.yield(.renew(liveID: liveID, authorization: replacement.capability)) else {
                try Task.checkCancellation()
                throw URLError(.timedOut)
            }
            var replies = acknowledgement.stream.makeAsyncIterator()
            guard try await replies.next() != nil else { throw CancellationError() }
            current = replacement
        }
    }
}

private actor NeonSocketActivity {
    private let clock = ContinuousClock()
    private var lastFrame = ContinuousClock.now
    private var canProbe = false

    func recordFrame() {
        lastFrame = clock.now
    }

    func enableProbes() {
        canProbe = true
    }

    func monitor(
        idle: Duration,
        timeout: Duration,
        outbound: AsyncStream<NeonClientMessage>.Continuation
    ) async throws {
        while true {
            let lastActivity = lastFrame
            try await clock.sleep(until: lastActivity.advanced(by: idle))
            guard lastFrame == lastActivity else { continue }
            if canProbe {
                outbound.yield(.heartbeatPing)
            }
            // The deadline is independent of the send task, which can also stall.
            try await clock.sleep(for: timeout)
            if lastFrame == lastActivity {
                throw URLError(.timedOut)
            }
        }
    }
}

protocol NeonWebSocket: Sendable {
    func negotiatedSubprotocol() async throws -> String?
    func send(_ text: String) async throws
    func receive() async throws -> String
    func close()
}

private struct URLSessionNeonWebSocket: NeonWebSocket {
    let task: URLSessionWebSocketTask
    private let handshake: AsyncThrowingStream<String?, Error>

    init(url: URL, session: URLSession) {
        let negotiation = AsyncThrowingStream<String?, Error>.makeStream(bufferingPolicy: .bufferingOldest(1))
        handshake = negotiation.stream
        task = session.webSocketTask(with: url, protocols: [neonRealtimeSubprotocol])
        task.delegate = NeonWebSocketHandshake(continuation: negotiation.continuation)
        // Neon can send messages larger than Foundation's default limit.
        task.maximumMessageSize = .max
        task.resume()
    }

    func negotiatedSubprotocol() async throws -> String? {
        var iterator = handshake.makeAsyncIterator()
        guard let selected = try await iterator.next() else {
            throw CancellationError()
        }
        return selected
    }

    func send(_ text: String) async throws {
        try await task.send(.string(text))
    }

    func receive() async throws -> String {
        guard case .string(let text) = try await task.receive() else {
            throw NeonRealtimeError.protocolViolation("Expected a text WebSocket message")
        }
        return text
    }

    func close() {
        task.cancel(with: .normalClosure, reason: nil)
    }
}

private final class NeonWebSocketHandshake: NSObject, URLSessionWebSocketDelegate {
    private let continuation: AsyncThrowingStream<String?, Error>.Continuation

    init(continuation: AsyncThrowingStream<String?, Error>.Continuation) {
        self.continuation = continuation
    }

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didOpenWithProtocol selectedProtocol: String?
    ) {
        continuation.yield(selectedProtocol)
        continuation.finish()
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: Error?
    ) {
        continuation.finish(throwing: error ?? URLError(.networkConnectionLost))
    }
}
