import Foundation
@testable import NeonRealtimeSwift
import Testing

private let rowA = String(repeating: "a", count: 64)
private let rowB = String(repeating: "b", count: 64)
private let visibility = #"{"xmin":"80","xmax":"101","xip":["90"]}"#
private let admission = """
{"type":"subscribed","request_id":"1","live_id":"7","epoch":"1","first_sequence":"5","columns":[
    {"name":"amount","type_oid":1700,"typmod":-1,"codec":"pg_text"},
    {"name":"payload","type_oid":17,"typmod":-1,"codec":"bytes"}]}
"""

@Test func subscriptionSendsV2HandshakeAndBundlesBufferedCommitsWithSnapshot() async throws {
    let socket = ScriptedWebSocket([
        #"{"type":"ping","token":"server-1"}"#,
        #"{"type":"ready"}"#,
        admission,
        baselineStart(),
        baselineBatch(
            index: 0,
            rows: #"[{"row_key":"\#(rowA)","values":["99999999999999999999.01",{"base64":"AAE="}]}]"#
        ),
        #"{"type":"progress","mvcc":\#(visibility)}"#,
        #"{"type":"open","publication_id":"p"}"#,
        results(index: 0, changes: #"[{"op":"upsert","row_key":"\#(rowB)","values":["3",null]}]"#),
        results(index: 1, changes: #"[{"op":"move","row_key":"\#(rowB)","after_row_key":null}]"#),
        commit(count: 2),
        baselineEnd(count: 1),
        #"{"type":"progress","mvcc":\#(visibility)}"#,
    ])
    let client = NeonRealtimeClient(url: try #require(URL(string: "wss://example.com/live"))) { _ in socket }
    let query = try JSONDecoder().decode(
        NeonSealedQuery.self,
        from: Data(#"{"capability":"sealed","queryFingerprint":"fingerprint","expiresAt":1234567890000}"#.utf8)
    )
    #expect(query.expiresAt == 1_234_567_890_000)

    var events: [NeonRealtimeEvent] = []
    do {
        for try await event in client.subscribe(to: query) {
            events.append(event)
        }
        Issue.record("Expected the fixture's disconnect to end the subscription")
    } catch ScriptEnd.disconnected {}

    try #require(events.count == 2)
    guard case .snapshot(let snapshot) = events[0],
          case .progress(let progress) = events[1] else {
        Issue.record("Expected a snapshot followed by progress")
        return
    }
    try #require(snapshot.bufferedBatches.count == 1)
    let batch = try #require(snapshot.bufferedBatches.first)
    #expect(snapshot.columns.map(\.typeOID) == [1700, 17])
    #expect(snapshot.rows == [NeonRow(rowID: rowA, values: [.text("99999999999999999999.01"), .bytes(Data([0, 1]))])])
    #expect(batch.changes == [
        .upsert(NeonRow(rowID: rowB, values: [.text("3"), .null])),
        .move(rowID: rowB, afterRowID: nil),
    ])
    #expect(batch.transactionIDs == ["18446744073709551615"])
    #expect(batch.lsn == "1/2")
    #expect(progress == snapshot.visibility)
    #expect(progress.includes(committedTransactionID: 78))
    #expect(!progress.includes(committedTransactionID: 90))
    #expect(!progress.includes(committedTransactionID: 101))

    let sent = try await socket.sent.map {
        try JSONDecoder().decode([String: String].self, from: Data($0.utf8))
    }
    #expect(sent == [
        ["type": "pong", "token": "server-1"],
        ["type": "client_info", "metadata": #"{"sdk":"swift"}"#],
        ["type": "subscribe", "request_id": "1", "authorization": "sealed"],
    ])
}

@Test(arguments: [nil, "neon.realtime.v1"] as [String?])
func subscriptionRequiresV2BeforeSendingTheCapability(selectedSubprotocol: String?) async throws {
    let socket = ScriptedWebSocket([#"{"type":"ready"}"#], selectedSubprotocol: selectedSubprotocol)
    let client = NeonRealtimeClient(url: try #require(URL(string: "wss://example.com/live"))) { _ in socket }
    let query = NeonSealedQuery(capability: "sealed", queryFingerprint: "fingerprint", expiresAt: 1)

    await #expect(throws: NeonRealtimeError.unexpectedSubprotocol(actual: selectedSubprotocol)) {
        var events = client.subscribe(to: query).makeAsyncIterator()
        _ = try await events.next()
    }
    #expect(await socket.sent.isEmpty)
}

@Test func supersededSnapshotAttemptKeepsCommittedBatchesInOrder() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(baselineStart(), to: &state)
    try apply(baselineBatch(index: 0, rows: #"[{"row_key":"\#(rowA)","values":["old",null]}]"#), to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    try apply(results(index: 0, transactionID: "101", changes: "[]"), to: &state)
    #expect(try apply(commit(count: 1), to: &state).isEmpty)

    try apply(baselineStart(attempt: 2), to: &state)
    try apply(baselineBatch(index: 0, attempt: 2, rows: #"[{"row_key":"\#(rowB)","values":["b",null]}]"#), to: &state)
    #expect(try apply(baselineEnd(count: 1), to: &state).isEmpty)
    try apply(#"{"type":"open","publication_id":"next"}"#, to: &state)
    try apply(results(index: 0, sequence: 6, publicationID: "next", transactionID: "102", changes: "[]"), to: &state)
    #expect(try apply(commit(count: 1, publicationID: "next"), to: &state).isEmpty)
    try apply(baselineBatch(index: 1, attempt: 2, rows: #"[{"row_key":"\#(rowA)","values":["a",null]}]"#), to: &state)

    let events = try apply(baselineEnd(attempt: 2, count: 2), to: &state)
    try #require(events.count == 1)
    guard case .snapshot(let snapshot) = events[0] else {
        Issue.record("Expected a complete replacement snapshot")
        return
    }
    #expect(snapshot.rows.map(\.rowID) == [rowB, rowA])
    #expect(snapshot.rows.map(\.values) == [[.text("b"), .null], [.text("a"), .null]])
    #expect(snapshot.bufferedBatches.map(\.transactionIDs) == [["101"], ["102"]])
    #expect(snapshot.bufferedBatches.allSatisfy { $0.changes.isEmpty })
}

@Test func resetPublishesOnlyAfterCommitAndDiscardsOldBaselineAndCommits() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(baselineStart(), to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    try apply(results(index: 0, changes: #"[{"op":"upsert","row_key":"\#(rowA)","values":["old",null]}]"#), to: &state)
    #expect(try apply(commit(count: 1), to: &state).isEmpty)

    try apply(#"{"type":"open","publication_id":"reset"}"#, to: &state)
    let reset = """
    {"type":"reset_required","publication_id":"reset","index":0,"targets":[
        {"live_id":"7","epoch":"2","first_sequence":"10"}]}
    """
    #expect(try apply(reset, to: &state).isEmpty)
    #expect(try apply(commit(count: 1, publicationID: "reset"), to: &state) == [.resetRequired])
    #expect(try apply(baselineEnd(count: 0), to: &state).isEmpty)
    try apply(baselineStart(epoch: 2), to: &state)
    #expect(try apply(#"{"type":"progress","mvcc":\#(visibility)}"#, to: &state).isEmpty)
    let events = try apply(baselineEnd(epoch: 2, count: 0), to: &state)
    guard case .snapshot(let snapshot)? = events.first else {
        Issue.record("An empty replacement still needs a snapshot event")
        return
    }
    #expect(snapshot.rows.isEmpty)
    #expect(snapshot.bufferedBatches.isEmpty)
}

@Test func disconnectDuringPublicationDoesNotEmitUncommittedChanges() async throws {
    let socket = ScriptedWebSocket([
        #"{"type":"ready"}"#,
        admission,
        baselineStart(),
        baselineEnd(count: 0),
        #"{"type":"open","publication_id":"p"}"#,
        results(index: 0, changes: #"[{"op":"upsert","row_key":"\#(rowA)","values":["uncommitted",null]}]"#),
    ])
    let client = NeonRealtimeClient(url: try #require(URL(string: "wss://example.com/live"))) { _ in socket }
    let query = NeonSealedQuery(capability: "sealed", queryFingerprint: "fingerprint", expiresAt: 1)
    var events = client.subscribe(to: query).makeAsyncIterator()
    guard case .snapshot(let snapshot)? = try await events.next() else {
        Issue.record("Expected the complete snapshot before disconnect")
        return
    }
    #expect(snapshot.rows.isEmpty)
    await #expect(throws: ScriptEnd.disconnected) {
        _ = try await events.next()
    }
}

@Test func progressInsideAnUncommittedPublicationIsRejected() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(baselineStart(), to: &state)
    try apply(baselineEnd(count: 0), to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    #expect(try apply(results(index: 0, changes: "[]"), to: &state).isEmpty)
    #expect(throws: NeonRealtimeError.protocolViolation("Publication interrupted before commit")) {
        try apply(#"{"type":"progress","mvcc":\#(visibility)}"#, to: &state)
    }
}

@Test func invalidMoveOrderAcrossFragmentsIsRejectedAtCommit() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(baselineStart(), to: &state)
    try apply(baselineEnd(count: 0), to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    #expect(try apply(results(index: 0, changes: #"[{"op":"move","row_key":"\#(rowB)","after_row_key":null}]"#), to: &state).isEmpty)
    #expect(try apply(results(index: 1, changes: #"[{"op":"upsert","row_key":"\#(rowA)","values":["late",null]}]"#), to: &state).isEmpty)
    #expect(throws: NeonRealtimeError.protocolViolation("Invalid upsert in ordered patch")) {
        try apply(commit(count: 2), to: &state)
    }
}

@Test func wrongCommitBodyCountCannotPublishPartialChanges() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(baselineStart(), to: &state)
    try apply(baselineEnd(count: 0), to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    #expect(try apply(results(index: 0, changes: #"[{"op":"remove","row_key":"\#(rowA)"}]"#), to: &state).isEmpty)
    #expect(throws: NeonRealtimeError.protocolViolation("Unexpected publication commit")) {
        try apply(commit(count: 2), to: &state)
    }
}

@Test func changedSequenceAcrossFragmentsIsRejected() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    try apply(results(index: 0, changes: "[]"), to: &state)
    #expect(throws: NeonRealtimeError.protocolViolation("Invalid publication change target")) {
        try apply(results(index: 1, sequence: 6, changes: "[]"), to: &state)
    }
}

@Test func incorrectCellCodecIsRejectedBeforeCommit() throws {
    var state = NeonSubscriptionState()
    try apply(admission, to: &state)
    try apply(#"{"type":"open","publication_id":"p"}"#, to: &state)
    #expect(throws: NeonRealtimeError.protocolViolation("Row value does not match its column codec")) {
        try apply(
            results(index: 0, changes: #"[{"op":"upsert","row_key":"\#(rowA)","values":["1","not bytes"]}]"#),
            to: &state
        )
    }
}

@Test
func subscriptionPreservesCancellationRetryHint() async throws {
    let socket = ScriptedWebSocket([
        #"{"type":"ready"}"#,
        admission,
        #"{"type":"subscription_error","live_id":"7","code":"upstream_cancelled","message":"Cancelled","retry_after_ms":4500}"#,
    ])
    let client = NeonRealtimeClient(url: try #require(URL(string: "wss://example.com/live"))) { _ in socket }
    let query = NeonSealedQuery(capability: "sealed", queryFingerprint: "fingerprint", expiresAt: 1)
    await #expect(throws: NeonRealtimeError.server(
        code: "upstream_cancelled",
        message: "Cancelled",
        sqlState: nil,
        retryAfterMilliseconds: 4_500
    )) {
        var events = client.subscribe(to: query).makeAsyncIterator()
        _ = try await events.next()
    }
}

@Test(.timeLimit(.minutes(1)))
func idleSocketProbesAndTimesOutWhenThePeerStopsResponding() async throws {
    let socket = WaitingWebSocket(initialFrames: [#"{"type":"ready"}"#])
    let client = NeonRealtimeClient(
        url: try #require(URL(string: "wss://example.com/live")),
        heartbeatIdle: .milliseconds(20),
        heartbeatTimeout: .milliseconds(500)
    ) { _ in socket }
    let query = NeonSealedQuery(capability: "sealed", queryFingerprint: "fingerprint", expiresAt: 1)

    let consumer = Task {
        var events = client.subscribe(to: query).makeAsyncIterator()
        return try await events.next()
    }
    defer { consumer.cancel() }

    var probes = socket.probes.stream.makeAsyncIterator()
    let token = try #require(await probes.next())
    socket.respond(to: token)
    try #require(await probes.next() != nil)

    await #expect(throws: URLError(.timedOut)) {
        _ = try await consumer.value
    }
    var closed = socket.closed.stream.makeAsyncIterator()
    try #require(await closed.next() != nil)
}

@Test(.timeLimit(.minutes(1)))
func cancellingIterationClosesTheSocket() async throws {
    let socket = WaitingWebSocket()
    let client = NeonRealtimeClient(url: try #require(URL(string: "wss://example.com/live"))) { _ in socket }
    let query = NeonSealedQuery(capability: "sealed", queryFingerprint: "fingerprint", expiresAt: 1)

    let consumer = Task {
        for try await _ in client.subscribe(to: query) {
            Issue.record("The blocked socket cannot produce an event")
        }
    }
    var receiving = socket.receiving.stream.makeAsyncIterator()
    try #require(await receiving.next() != nil)
    consumer.cancel()
    try await consumer.value
    var closed = socket.closed.stream.makeAsyncIterator()
    try #require(await closed.next() != nil)
}

@discardableResult
private func apply(_ json: String, to state: inout NeonSubscriptionState) throws -> [NeonRealtimeEvent] {
    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase
    return try state.apply(decoder.decode(NeonServerMessage.self, from: Data(json.utf8)))
}

private func baselineStart(epoch: Int = 1, attempt: Int = 1) -> String {
    #"{"type":"baseline_sync_start","live_id":"7","epoch":"\#(epoch)","baseline_sync_attempt":"\#(attempt)","mvcc":\#(visibility)}"#
}

private func baselineBatch(index: Int, attempt: Int = 1, rows: String) -> String {
    #"{"type":"baseline_sync_batch","live_id":"7","epoch":"1","baseline_sync_attempt":"\#(attempt)","index":\#(index),"rows":\#(rows)}"#
}

private func baselineEnd(epoch: Int = 1, attempt: Int = 1, count: Int) -> String {
    #"{"type":"baseline_sync_end","live_id":"7","epoch":"\#(epoch)","baseline_sync_attempt":"\#(attempt)","batch_count":\#(count)}"#
}

private func results(
    index: Int,
    sequence: Int = 5,
    publicationID: String = "p",
    transactionID: String = "18446744073709551615",
    changes: String
) -> String {
    """
    {"type":"keyed_results","publication_id":"\(publicationID)","index":\(index),"txids":["\(transactionID)"],
     "targets":[{"live_id":"7","epoch":"1","sequence":"\(sequence)"}],"changes":\(changes)}
    """
}

private func commit(count: Int, publicationID: String = "p") -> String {
    #"{"type":"commit","publication_id":"\#(publicationID)","body_count":\#(count),"frontier":{"lsn":"1/2"}}"#
}

private enum ScriptEnd: Error {
    case disconnected
}

private actor ScriptedWebSocket: NeonWebSocket {
    private var frames: ArraySlice<String>
    private let selectedSubprotocol: String?
    private(set) var sent: [String] = []

    init(_ frames: [String], selectedSubprotocol: String? = "neon.realtime.v2") {
        self.frames = frames[...]
        self.selectedSubprotocol = selectedSubprotocol
    }

    func negotiatedSubprotocol() -> String? {
        selectedSubprotocol
    }

    func send(_ text: String) {
        sent.append(text)
    }

    func receive() throws -> String {
        guard let frame = frames.popFirst() else { throw ScriptEnd.disconnected }
        return frame
    }

    nonisolated func close() {}
}

private struct WaitingWebSocket: NeonWebSocket {
    let receiving = AsyncStream<Void>.makeStream()
    let closed = AsyncStream<Void>.makeStream()
    let probes = AsyncStream<String>.makeStream()
    private let frames = AsyncStream<String>.makeStream()

    init(initialFrames: [String] = []) {
        for frame in initialFrames {
            frames.continuation.yield(frame)
        }
    }

    func negotiatedSubprotocol() -> String? { "neon.realtime.v2" }

    func send(_ text: String) throws {
        let message = try JSONDecoder().decode([String: String].self, from: Data(text.utf8))
        if message["type"] == "ping", let token = message["token"] {
            probes.continuation.yield(token)
        }
    }

    func respond(to token: String) {
        frames.continuation.yield(#"{"type":"pong","token":"\#(token)"}"#)
    }

    func receive() async throws -> String {
        receiving.continuation.yield(())
        var iterator = frames.stream.makeAsyncIterator()
        guard let frame = await iterator.next() else { throw ScriptEnd.disconnected }
        return frame
    }

    func close() {
        frames.continuation.finish()
        probes.continuation.finish()
        closed.continuation.yield(())
        closed.continuation.finish()
    }
}
