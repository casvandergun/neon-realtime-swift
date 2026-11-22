import Foundation

// Neon encodes these integers as canonical nonzero decimal strings.
struct NeonWireID: Decodable, Equatable {
    let value: UInt64

    init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        let text = try container.decode(String.self)
        guard let number = UInt64(text), number > 0, String(number) == text else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid decimal identifier")
        }
        value = number
    }
}

private struct WireRowKey: Decodable {
    let value: String

    init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        let text = try container.decode(String.self)
        guard text.utf8.count == 64,
              text.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) }) else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid row key")
        }
        value = text
    }
}

enum NeonServerMessage: Decodable {
    case ready
    case ping(String)
    case pong
    case subscribed(Admission)
    case baselineStart(BaselineVersion, NeonTransactionVisibility)
    case baselineBatch(BaselineVersion, index: UInt32, rows: [NeonRow])
    case baselineEnd(BaselineVersion, batchCount: UInt32)
    case open(String)
    case results(Results)
    case reset(Reset)
    case commit(Commit)
    case progress(NeonTransactionVisibility)
    case warning
    case failure(Failure)
    case renewed
    case unsubscribed

    struct Admission: Decodable {
        struct Column: Decodable {
            let name: String
            let typeOid: UInt32
            let typmod: Int32
            let codec: NeonColumn.Codec

            var value: NeonColumn {
                NeonColumn(name: name, typeOID: typeOid, typeModifier: typmod, codec: codec)
            }
        }

        let requestId: NeonWireID
        let liveId: NeonWireID
        let epoch: NeonWireID
        let firstSequence: NeonWireID
        let columns: [Column]
    }

    struct BaselineVersion: Decodable, Equatable {
        let liveId: NeonWireID
        let epoch: NeonWireID
        let baselineSyncAttempt: NeonWireID
    }

    struct Results: Decodable {
        struct Target: Decodable {
            let liveId: NeonWireID
            let epoch: NeonWireID
            let sequence: NeonWireID
        }

        let publicationId: String
        let index: UInt32
        let txids: [NeonWireID]
        let targets: [Target]
        let changes: [WireChange]
    }

    struct Reset: Decodable {
        struct Target: Decodable {
            let liveId: NeonWireID
            let epoch: NeonWireID
            let firstSequence: NeonWireID
        }

        let publicationId: String
        let index: UInt32
        let targets: [Target]
    }

    struct Commit: Decodable {
        struct Frontier: Decodable {
            let lsn: String
        }

        let publicationId: String
        let bodyCount: UInt32
        let frontier: Frontier
    }

    struct Failure: Decodable {
        let requestId: NeonWireID?
        let liveId: NeonWireID?
        let code: String
        let message: String
        let sqlstate: String?
        let retryAfterMs: UInt32?

        var error: NeonRealtimeError {
            .server(code: code, message: message, sqlState: sqlstate, retryAfterMilliseconds: retryAfterMs)
        }
    }

    private struct Visibility: Decodable {
        let xmin: NeonWireID
        let xmax: NeonWireID
        let xip: [NeonWireID]

        func validated() throws -> NeonTransactionVisibility {
            let lower = xmin.value
            let upper = xmax.value
            let inProgress = Set(xip.map(\.value))
            guard lower <= upper, inProgress.allSatisfy({ $0 >= lower && $0 < upper }) else {
                throw NeonRealtimeError.protocolViolation("Invalid transaction visibility")
            }
            return NeonTransactionVisibility(xmin: lower, xmax: upper, inProgress: inProgress)
        }
    }

    private struct WireRow: Decodable {
        let rowKey: WireRowKey
        let values: [NeonCell]

        var value: NeonRow { NeonRow(rowID: rowKey.value, values: values) }
    }

    struct WireChange: Decodable {
        let value: NeonChange

        private enum Operation: String, Decodable {
            case upsert, remove, move
        }

        private enum CodingKeys: String, CodingKey {
            case op, rowKey, values, afterRowKey
        }

        init(from decoder: any Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let key = try container.decode(WireRowKey.self, forKey: .rowKey).value
            switch try container.decode(Operation.self, forKey: .op) {
            case .upsert:
                let values = try container.decode([NeonCell].self, forKey: .values)
                value = .upsert(NeonRow(rowID: key, values: values))
            case .remove:
                value = .remove(rowID: key)
            case .move:
                guard container.contains(.afterRowKey) else {
                    throw DecodingError.keyNotFound(
                        CodingKeys.afterRowKey,
                        .init(codingPath: decoder.codingPath, debugDescription: "Missing move predecessor")
                    )
                }
                let predecessor = try container.decodeIfPresent(WireRowKey.self, forKey: .afterRowKey)
                value = .move(rowID: key, afterRowID: predecessor?.value)
            }
        }
    }

    private enum Kind: String, Decodable {
        case ready, ping, pong, subscribed, open, commit, progress, renewed, unsubscribed
        case baselineStart = "baseline_sync_start"
        case baselineBatch = "baseline_sync_batch"
        case baselineEnd = "baseline_sync_end"
        case results = "keyed_results"
        case reset = "reset_required"
        case warning = "client_warning"
        case subscribeRejected = "subscribe_rejected"
        case subscriptionError = "subscription_error"
        case connectionError = "connection_error"
    }

    private enum CodingKeys: String, CodingKey {
        case type, token, publicationId, mvcc, index, rows, batchCount, requestId, liveId, code, message
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(Kind.self, forKey: .type) {
        case .ready:
            self = .ready
        case .ping:
            self = .ping(try container.decode(String.self, forKey: .token))
        case .pong:
            _ = try container.decode(String.self, forKey: .token)
            self = .pong
        case .subscribed:
            self = .subscribed(try Admission(from: decoder))
        case .baselineStart:
            self = .baselineStart(
                try BaselineVersion(from: decoder),
                try container.decode(Visibility.self, forKey: .mvcc).validated()
            )
        case .baselineBatch:
            self = .baselineBatch(
                try BaselineVersion(from: decoder),
                index: try container.decode(UInt32.self, forKey: .index),
                rows: try container.decode([WireRow].self, forKey: .rows).map(\.value)
            )
        case .baselineEnd:
            self = .baselineEnd(
                try BaselineVersion(from: decoder),
                batchCount: try container.decode(UInt32.self, forKey: .batchCount)
            )
        case .open:
            self = .open(try container.decode(String.self, forKey: .publicationId))
        case .results:
            self = .results(try Results(from: decoder))
        case .reset:
            self = .reset(try Reset(from: decoder))
        case .commit:
            self = .commit(try Commit(from: decoder))
        case .progress:
            self = .progress(try container.decode(Visibility.self, forKey: .mvcc).validated())
        case .warning:
            _ = try container.decode(String.self, forKey: .code)
            _ = try container.decode(String.self, forKey: .message)
            self = .warning
        case .subscribeRejected:
            _ = try container.decode(NeonWireID.self, forKey: .requestId)
            self = .failure(try Failure(from: decoder))
        case .subscriptionError:
            _ = try container.decode(NeonWireID.self, forKey: .liveId)
            self = .failure(try Failure(from: decoder))
        case .connectionError:
            self = .failure(try Failure(from: decoder))
        case .renewed:
            self = .renewed
        case .unsubscribed:
            self = .unsubscribed
        }
    }
}
