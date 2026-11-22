// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "NeonRealtimeSwift",
    platforms: [
        .iOS(.v16),
        .macOS(.v13),
    ],
    products: [
        .library(name: "NeonRealtimeSwift", targets: ["NeonRealtimeSwift"]),
    ],
    targets: [
        .target(name: "NeonRealtimeSwift"),
        .testTarget(
            name: "NeonRealtimeSwiftTests",
            dependencies: ["NeonRealtimeSwift"]
        ),
    ]
)
