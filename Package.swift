// swift-tools-version:4.0

import PackageDescription
let package = Package(
    name: "PostgresStORM",
    products: [
        .library(name: "PostgresStORM", targets: ["PostgresStORM"])
    ],
    dependencies: [
        .package(url: "https://github.com/PerfectlySoft/Perfect-PostgreSQL.git", from: "3.0.0"),
        .package(url: "https://github.com/sage444/StORM.git", .branch("master")),
        ],
    targets: [
        .target(name: "PostgresStORM", dependencies: ["PerfectPostgreSQL", "StORM"], path: "Sources")
    ]
)
