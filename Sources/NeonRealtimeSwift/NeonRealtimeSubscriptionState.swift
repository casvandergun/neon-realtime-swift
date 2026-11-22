struct NeonSubscriptionState {
    private struct Baseline {
        let version: NeonServerMessage.BaselineVersion
        let visibility: NeonTransactionVisibility
        var nextBatch: UInt64 = 0
        var rows: [NeonRow] = []
        var keys: Set<String> = []
    }

    private struct Publication {
        let id: String
        var nextBody: UInt64 = 0
        var transactionIDs: [NeonWireID]?
        var changes: [NeonChange] = []
        var reset: NeonServerMessage.Reset.Target?
    }

    static let requestID = "1"

    private var liveID: NeonWireID?
    private var epoch: UInt64 = 0
    private var nextSequence: UInt64?
    private var columns: [NeonColumn] = []
    private var latestBaselineAttempt: UInt64 = 0
    private var baseline: Baseline?
    private var backlog: [NeonChangeBatch] = []
    private var publication: Publication?
    private var hasSnapshot = false

    mutating func apply(_ message: NeonServerMessage) throws -> [NeonRealtimeEvent] {
        switch message {
        case .subscribed(let admission):
            try admit(admission)
        case .baselineStart(let version, let visibility):
            try requireOutsidePublication()
            guard try isCurrent(version) else { return [] }
            guard !hasSnapshot, version.baselineSyncAttempt.value > latestBaselineAttempt else {
                throw NeonRealtimeError.protocolViolation("Unexpected baseline start")
            }
            latestBaselineAttempt = version.baselineSyncAttempt.value
            baseline = Baseline(version: version, visibility: visibility)
        case .baselineBatch(let version, let index, let rows):
            try requireOutsidePublication()
            guard try isCurrent(version) else { return [] }
            guard baseline?.version == version, baseline?.nextBatch == UInt64(index) else {
                throw NeonRealtimeError.protocolViolation("Unexpected baseline batch")
            }
            for row in rows {
                try validate(row)
                guard baseline?.keys.insert(row.rowID).inserted == true else {
                    throw NeonRealtimeError.protocolViolation("Duplicate baseline row ID")
                }
            }
            baseline?.rows.append(contentsOf: rows)
            baseline?.nextBatch += 1
        case .baselineEnd(let version, let batchCount):
            try requireOutsidePublication()
            guard try isCurrent(version) else { return [] }
            guard let baseline, baseline.version == version,
                  baseline.nextBatch == UInt64(batchCount) else {
                throw NeonRealtimeError.protocolViolation("Unexpected baseline end")
            }
            self.baseline = nil
            hasSnapshot = true
            // Publications received during baseline sync are newer than that baseline.
            let snapshot = NeonSnapshot(
                columns: columns,
                rows: baseline.rows,
                visibility: baseline.visibility,
                bufferedBatches: backlog
            )
            backlog.removeAll()
            return [.snapshot(snapshot)]
        case .open(let id):
            guard publication == nil, !id.isEmpty, id.utf8.count <= 64 else {
                throw NeonRealtimeError.protocolViolation("Invalid publication opening")
            }
            publication = Publication(id: id)
        case .results(let results):
            try stage(results)
        case .reset(let reset):
            try stage(reset)
        case .commit(let commit):
            return try finish(commit)
        case .progress(let visibility):
            try requireOutsidePublication()
            return hasSnapshot ? [.progress(visibility)] : []
        case .failure(let failure):
            try validate(failure)
            throw failure.error
        case .warning, .pong:
            break
        case .ready, .ping, .renewed, .unsubscribed:
            throw NeonRealtimeError.protocolViolation("Unexpected server message")
        }
        return []
    }

    private mutating func admit(_ admission: NeonServerMessage.Admission) throws {
        guard liveID == nil, String(admission.requestId.value) == Self.requestID else {
            throw NeonRealtimeError.protocolViolation("Unexpected subscription admission")
        }
        let columns = admission.columns.map(\.value)
        var names = Set<[UInt8]>()
        guard !columns.isEmpty, columns.allSatisfy({ column in
            let expectedCodec: NeonColumn.Codec = column.typeOID == 17 ? .bytes : .postgresText
            return !column.name.isEmpty && names.insert(Array(column.name.utf8)).inserted &&
                column.typeOID > 0 && column.codec == expectedCodec
        }) else {
            throw NeonRealtimeError.protocolViolation("Invalid result columns")
        }
        liveID = admission.liveId
        epoch = admission.epoch.value
        nextSequence = admission.firstSequence.value
        self.columns = columns
    }

    private func isCurrent(_ version: NeonServerMessage.BaselineVersion) throws -> Bool {
        guard liveID == version.liveId, version.epoch.value <= epoch else {
            throw NeonRealtimeError.protocolViolation("Invalid baseline subscription or epoch")
        }
        return version.epoch.value == epoch && version.baselineSyncAttempt.value >= latestBaselineAttempt
    }

    private func requireOutsidePublication() throws {
        guard publication == nil else {
            throw NeonRealtimeError.protocolViolation("Publication interrupted before commit")
        }
    }

    private mutating func advanceBody(id: String, index: UInt32) throws {
        guard publication?.id == id, publication?.nextBody == UInt64(index) else {
            throw NeonRealtimeError.protocolViolation("Unexpected publication body")
        }
        publication?.nextBody += 1
    }

    private mutating func stage(_ results: NeonServerMessage.Results) throws {
        try advanceBody(id: results.publicationId, index: results.index)
        guard !results.targets.isEmpty else { return }
        guard results.targets.count == 1, let target = results.targets.first,
              target.liveId == liveID, target.epoch.value == epoch,
              target.sequence.value == nextSequence, publication?.reset == nil,
              publication?.transactionIDs == nil || publication?.transactionIDs == results.txids else {
            throw NeonRealtimeError.protocolViolation("Invalid publication change target")
        }
        let changes = results.changes.map(\.value)
        for case .upsert(let row) in changes {
            try validate(row)
        }
        publication?.transactionIDs = results.txids
        publication?.changes.append(contentsOf: changes)
    }

    private mutating func stage(_ reset: NeonServerMessage.Reset) throws {
        try advanceBody(id: reset.publicationId, index: reset.index)
        guard !reset.targets.isEmpty else { return }
        guard reset.targets.count == 1, let target = reset.targets.first,
              target.liveId == liveID, target.epoch.value > epoch,
              publication?.transactionIDs == nil, publication?.reset == nil else {
            throw NeonRealtimeError.protocolViolation("Invalid publication reset target")
        }
        publication?.reset = target
    }

    private mutating func finish(_ commit: NeonServerMessage.Commit) throws -> [NeonRealtimeEvent] {
        guard let publication, publication.id == commit.publicationId,
              publication.nextBody == UInt64(commit.bodyCount) else {
            throw NeonRealtimeError.protocolViolation("Unexpected publication commit")
        }
        self.publication = nil
        if let reset = publication.reset {
            epoch = reset.epoch.value
            nextSequence = reset.firstSequence.value
            latestBaselineAttempt = 0
            baseline = nil
            backlog.removeAll()
            hasSnapshot = false
            return [.resetRequired]
        }
        guard let transactionIDs = publication.transactionIDs, let sequence = nextSequence else {
            return []
        }
        try validateMoveOrder(publication.changes)
        nextSequence = sequence == UInt64.max ? nil : sequence + 1
        let batch = NeonChangeBatch(
            changes: publication.changes,
            transactionIDs: transactionIDs.map { String($0.value) },
            lsn: commit.frontier.lsn
        )
        if hasSnapshot {
            return [.changes(batch)]
        }
        backlog.append(batch)
        return []
    }

    private func validate(_ row: NeonRow) throws {
        guard row.values.count == columns.count else {
            throw NeonRealtimeError.protocolViolation("Row value count differs from columns")
        }
        for (cell, column) in zip(row.values, columns) {
            switch (cell, column.codec) {
            case (.text, .bytes), (.bytes, .postgresText):
                throw NeonRealtimeError.protocolViolation("Row value does not match its column codec")
            default:
                break
            }
        }
    }

    private func validate(_ failure: NeonServerMessage.Failure) throws {
        if let requested = failure.requestId,
           liveID != nil || String(requested.value) != Self.requestID {
            throw NeonRealtimeError.protocolViolation("Rejection for an unknown request")
        }
        if let rejected = failure.liveId, rejected != liveID {
            throw NeonRealtimeError.protocolViolation("Error for an unknown subscription")
        }
    }
}

// Check the whole ordered patch before publishing it. Membership-dependent
// checks belong to the consumer, which owns the installed result.
private func validateMoveOrder(_ changes: [NeonChange]) throws {
    let moved = Set(changes.compactMap { change -> String? in
        if case .move(let id, _) = change { return id }
        return nil
    })
    guard !moved.isEmpty else { return }

    var contents = Set<String>()
    var removed = Set<String>()
    var placed = Set<String>()
    var predecessors = Set<String?>()
    for change in changes {
        switch change {
        case .upsert(let row):
            guard placed.isEmpty, contents.insert(row.rowID).inserted else {
                throw NeonRealtimeError.protocolViolation("Invalid upsert in ordered patch")
            }
        case .remove(let id):
            guard placed.isEmpty, contents.insert(id).inserted else {
                throw NeonRealtimeError.protocolViolation("Invalid removal in ordered patch")
            }
            removed.insert(id)
        case .move(let id, let predecessor):
            guard id != predecessor, !removed.contains(id),
                  predecessors.insert(predecessor).inserted else {
                throw NeonRealtimeError.protocolViolation("Invalid move in ordered patch")
            }
            if let predecessor,
               removed.contains(predecessor) || (moved.contains(predecessor) && !placed.contains(predecessor)) {
                throw NeonRealtimeError.protocolViolation("Invalid move predecessor")
            }
            guard placed.insert(id).inserted else {
                throw NeonRealtimeError.protocolViolation("Duplicate move in ordered patch")
            }
        }
    }
}
