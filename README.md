# Jito Bundle Test Server

A test server implementation for the Jito Block Engine protocol, supporting both bundle and packet subscriptions.

## Features

- Bundle generation with random transfer transactions
- Optional packet subscription with transfer transactions
- Configurable compute unit limits and prices
- Automatic blockhash updates
- Authentication service implementation

## Prerequisites

- Rust 1.70 or later
- Solana CLI tools (for keypair management)
- Protobuf compiler (protoc)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/jito-bundle-test-server.git
cd jito-bundle-test-server
```

2. Build the project:
```bash
cargo build --release
```

## Configuration

The server can be configured using command-line arguments:

| Argument | Description | Default Value |
|----------|-------------|---------------|
| `--solana-rpc-url` | Solana RPC endpoint URL | http://127.0.0.1:8899 |
| `--keypair-path` | Path to the keypair file | id.json |
| `--be-server-addr` | Server listening address | 127.0.0.1:50051 |
| `--blockhash-update-interval-ms` | Blockhash update interval in milliseconds | 1000 |
| `--bundle-generation-interval-ms` | Bundle generation interval in milliseconds | 50 |
| `--compute-unit-limit` | Compute unit limit for transactions | 1400000 |
| `--compute-unit-price` | Compute unit price for transactions | 10000 |
| `--enable-packets-subscription` | Enable packet subscription | false |

## Usage

### Basic Usage

Run the server with default settings:
```bash
cargo run
```

### Enable Packet Subscription

To enable packet subscription:
```bash
cargo run -- --enable-packets-subscription
```

### Custom Configuration Example

```bash
cargo run -- \
    --solana-rpc-url https://api.mainnet-beta.solana.com \
    --keypair-path /path/to/keypair.json \
    --be-server-addr 0.0.0.0:50051 \
    --blockhash-update-interval-ms 2000 \
    --bundle-generation-interval-ms 100 \
    --compute-unit-limit 2000000 \
    --compute-unit-price 5000 \
    --enable-packets-subscription
```

## Transaction Details

The server generates two types of transactions:

1. **Bundle Transactions**:
   - Contains 3 transfer transactions
   - Random amounts between 10,000 and 20,000 lamports
   - Includes memo instruction
   - Uses compute budget instructions

2. **Packet Transactions** (when enabled):
   - Single transfer transaction
   - Random amount between 10,000 and 20,000 lamports
   - Includes memo instruction
   - Uses compute budget instructions

## Protocol Support

The server implements the following Jito Block Engine protocol services:

- `BlockEngineValidator`: For bundle and packet subscriptions
- `AuthService`: For authentication

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Build with debug info
cargo build --profile release-with-debug
```
