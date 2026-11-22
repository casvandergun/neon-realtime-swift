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

    /// Creates a logical subscription. The loader supplies a fresh capability for
    /// the same query on every connection attempt.
    public func subscription(
        loadQuery: @escaping @Sendable () async throws -> SealedLiveQuery
    ) -> RawLiveQuerySubscription {
        RawLiveQuerySubscription(loadQuery: loadQuery, subscribe: subscribe(to:))
    }

    /// Opens a connection for one sealed query and emits updates in application order.
    /// Cancel the consuming task to close the connection. On expiry or disconnect,
    /// subscribe again with a fresh sealed query to obtain a replacement snapshot.
    public func subscribe(to query: SealedLiveQuery) -> AsyncThrowingStream<NeonRealtimeEvent, Error> {
        AsyncThrowingStream { continuation in
            guard isSupportedEndpoint else {
                continuation.finish(throwing: NeonRealtimeError.invalidEndpoint)
                return
            }
            guard !query.capability.isEmpty, query.capability.utf8.count <= 262_144 else {
                continuation.finish(throwing: NeonRealtimeError.invalidSealedQuery)
                return
            }

            let socket = makeSocket(url)
            let task = Task {
                do {
                    try await runSubscription(query.capability, on: socket, into: continuation)
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
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
        _ capability: String,
        on socket: any NeonWebSocket,
        into continuation: AsyncThrowingStream<NeonRealtimeEvent, Error>.Continuation
    ) async throws {
        let activity = NeonSocketActivity()
        let probes = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        try await withThrowingTaskGroup(of: Void.self) { group in
            defer {
                group.cancelAll()
                probes.continuation.finish()
                // Close before the group waits for a blocked receive or send.
                socket.close()
            }
            group.addTask {
                try await Self.receive(capability, from: socket, into: continuation, activity: activity)
            }
            group.addTask {
                let encoder = JSONEncoder()
                for await _ in probes.stream {
                    try Task.checkCancellation()
                    try await Self.send(.heartbeatPing, through: socket, encoder: encoder)
                }
            }
            group.addTask {
                try await activity.monitor(idle: heartbeatIdle, timeout: heartbeatTimeout, probes: probes.continuation)
            }
            _ = try await group.next()
        }
    }

    private static func receive(
        _ capability: String,
        from socket: any NeonWebSocket,
        into continuation: AsyncThrowingStream<NeonRealtimeEvent, Error>.Continuation,
        activity: NeonSocketActivity
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
            default:
                guard ready else {
                    throw NeonRealtimeError.protocolViolation("Expected Realtime ready")
                }
                for event in try state.apply(message) {
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
    var authorization: String?
    var token: String?

    static let clientInfo = Self(type: "client_info", metadata: #"{"sdk":"swift"}"#)
    static let heartbeatPing = Self(type: "ping", token: "swift-heartbeat")

    static func subscribe(authorization: String) -> Self {
        Self(type: "subscribe", requestId: NeonSubscriptionState.requestID, authorization: authorization)
    }

    static func pong(token: String) -> Self {
        Self(type: "pong", token: token)
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

    func monitor(idle: Duration, timeout: Duration, probes: AsyncStream<Void>.Continuation) async throws {
        while true {
            let lastActivity = lastFrame
            try await clock.sleep(until: lastActivity.advanced(by: idle))
            guard lastFrame == lastActivity else { continue }
            if canProbe {
                probes.yield(())
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
