import Foundation

/// The JSON response from an application endpoint that seals a live SQL query.
public struct NeonSealedQuery: Codable, Sendable {
    public let capability: String
    public let queryFingerprint: String
    /// Unix time in milliseconds.
    public let expiresAt: Int64

    public init(capability: String, queryFingerprint: String, expiresAt: Int64) {
        self.capability = capability
        self.queryFingerprint = queryFingerprint
        self.expiresAt = expiresAt
    }
}

public struct NeonColumn: Sendable, Equatable {
    public enum Codec: String, Sendable, Decodable {
        case postgresText = "pg_text"
        case bytes
    }

    public let name: String
    public let typeOID: UInt32
    public let typeModifier: Int32
    public let codec: Codec
}

/// A raw cell. Text retains PostgreSQL's wire representation and precision.
public enum NeonCell: Sendable, Equatable, Decodable {
    case null
    case text(String)
    case bytes(Data)

    public init(from decoder: any Decoder) throws {
        struct BinaryCell: Decodable {
            let base64: String
        }

        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let text = try? container.decode(String.self) {
            self = .text(text)
        } else {
            let encoded = try container.decode(BinaryCell.self).base64
            guard let bytes = Data(base64Encoded: encoded),
                  bytes.base64EncodedString() == encoded else {
                throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid base64 cell")
            }
            self = .bytes(bytes)
        }
    }
}

public struct NeonRow: Sendable, Equatable {
    /// An opaque ID scoped to this subscription's current snapshot.
    public let rowID: String
    /// Positional values in the order of the snapshot's columns.
    public let values: [NeonCell]
}

public enum NeonChange: Sendable, Equatable {
    case upsert(NeonRow)
    /// Removes a row from the query result.
    case remove(rowID: String)
    /// Places the row after its predecessor, or at the head for a nil predecessor.
    case move(rowID: String, afterRowID: String?)
}

/// Visibility evidence for transactions already known to have committed.
public struct NeonTransactionVisibility: Sendable, Equatable {
    public let xmin: UInt64
    public let xmax: UInt64
    public let inProgress: Set<UInt64>

    public func includes(committedTransactionID id: UInt64) -> Bool {
        id > 0 && id < xmax && !inProgress.contains(id)
    }
}

public struct NeonSnapshot: Sendable, Equatable {
    public let columns: [NeonColumn]
    /// The complete replacement result, in server order, including an empty result.
    public let rows: [NeonRow]
    public let visibility: NeonTransactionVisibility
    /// Committed batches that arrived during snapshot assembly, in application order.
    public let bufferedBatches: [NeonChangeBatch]
}

public struct NeonChangeBatch: Sendable, Equatable {
    public let changes: [NeonChange]
    /// Exact decimal PostgreSQL transaction IDs.
    public let transactionIDs: [String]
    public let lsn: String
}

public enum NeonRealtimeEvent: Sendable, Equatable {
    case snapshot(NeonSnapshot)
    case changes(NeonChangeBatch)
    /// Applies once the consumer has installed the preceding events.
    case progress(NeonTransactionVisibility)
    /// A replacement snapshot is pending. The previous result can still be shown.
    case resetRequired
}

public enum NeonRealtimeError: Error, Sendable, Equatable {
    case invalidEndpoint
    case invalidSealedQuery
    case unexpectedSubprotocol(actual: String?)
    case protocolViolation(String)
    case server(code: String, message: String, sqlState: String?, retryAfterMilliseconds: UInt32?)
}
