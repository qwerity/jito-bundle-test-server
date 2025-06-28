# Jito Bundle Test Server

A test server and client implementation for the Jito Block Engine protocol, supporting both bundle and packet subscriptions with authentication and automatic reconnection.

## Features

### Server (`bundlesvr`)
- Bundle generation with random transfer transactions (1-5 transactions per bundle)
- Packet subscription with transfer transactions
- Configurable compute unit limits and prices
- Automatic blockhash updates from Solana RPC
- Authentication service implementation
- Configurable generation intervals
- Bundle hash calculation for verification
- Fee info endpoint implementation

### Client (`bundle_client`)
- Authenticates with server using keypair-based challenge-response
- Subscribes to bundles and packets from server
- Displays bundle counts and hashes for verification
- Automatic reconnection with exponential backoff retry logic
- Smart token management (reuses valid tokens)
- Handles both connection failures and authentication errors
- Parallel subscription support (can run both bundle and packet subscriptions)

## Binaries

This project builds two binaries:

1. **`bundlesvr`** - The bundle server that generates and serves bundles/packets
2. **`bundle_client`** - A client that connects to the server and receives bundles/packets

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

### Server Configuration (`bundlesvr`)

| Argument | Description | Default Value |
|----------|-------------|---------------|
| `--solana-rpc-url` | Solana RPC endpoint URL | https://api.testnet.solana.com |
| `--keypair-path` | Path to the keypair file | id.json |
| `--bind-ip` | Server binding IP address | 127.0.0.1 |
| `--bind-port` | Server listening port | 31001 |
| `--blockhash-update-interval-ms` | Blockhash update interval in milliseconds | 1000 |
| `--packets-generation-interval-ms` | Packet generation interval in milliseconds | 50 |
| `--bundle-generation-interval-ms` | Bundle generation interval in milliseconds | 50 |
| `--compute-unit-limit` | Compute unit limit for transactions | 1400000 |
| `--compute-unit-price` | Compute unit price for transactions | 10000 |
| `--disable-packets-subscription` | Disable packet subscription | false |
| `--disable-bundles-subscription` | Disable bundle subscription | false |

### Client Configuration (`bundle_client`)

| Argument | Description | Default Value |
|----------|-------------|---------------|
| `--bind-ip` | Server IP address to connect to | 127.0.0.1 |
| `--bind-port` | Server port to connect to | 31001 |
| `--keypair-path` | Path to the keypair file | id.json |
| `--disable-packets-subscription` | Disable packet subscription | false |
| `--disable-bundles-subscription` | Disable bundle subscription | false |

## Usage

### Running the Server

**Basic usage with defaults:**
```bash
cargo run --bin bundlesvr
```

**Custom IP and port:**
```bash
cargo run --bin bundlesvr --bind-ip 0.0.0.0 --bind-port 8080
```

**With custom Solana RPC and faster generation:**
```bash
cargo run --bin bundlesvr \
    --solana-rpc-url https://api.mainnet-beta.solana.com \
    --bind-ip 0.0.0.0 \
    --bind-port 31001 \
    --bundle-generation-interval-ms 25 \
    --packets-generation-interval-ms 25
```

**Disable packet subscriptions:**
```bash
cargo run --bin bundlesvr --disable-packets-subscription
```

### Running the Client

**Basic usage (connects to localhost:31001):**
```bash
cargo run --bin bundle_client
```

**Connect to remote server:**
```bash
cargo run --bin bundle_client --bind-ip 192.168.1.100 --bind-port 8080
```

**Only subscribe to bundles:**
```bash
cargo run --bin bundle_client --disable-packets-subscription
```

**Only subscribe to packets:**
```bash
cargo run --bin bundle_client --disable-bundles-subscription
```

### Complete Example

**Terminal 1 - Start server:**
```bash
cargo run --bin bundlesvr --bind-ip 127.0.0.1 --bind-port 31001
```

**Terminal 2 - Start client:**
```bash
cargo run --bin bundle_client --bind-ip 127.0.0.1 --bind-port 31001
```

## Transaction Details

The server generates realistic Solana transactions:

### Bundle Transactions
- Random number of transactions (1-5 per bundle)
- Each transaction transfers between 10,000-20,000 lamports
- Includes compute budget instructions (limit and price)
- Includes memo instruction with transfer details
- Self-transfers from keypair to itself
- Each bundle gets a unique UUID and deterministic hash

### Packet Transactions
- Single transfer transaction per packet
- Random amount between 10,000-20,000 lamports
- Same structure as bundle transactions
- Generated at configurable intervals

## Authentication Flow

1. **Challenge Generation**: Client requests a challenge from server using its public key
2. **Keypair Signing**: Client signs a message containing its public key + challenge
3. **Token Generation**: Server validates signature and issues access tokens
4. **API Access**: Client uses Bearer token for all subsequent requests
5. **Auto-Renewal**: Client automatically re-authenticates when tokens expire

The authentication is role-based (Validator role) and uses the Ed25519 keypair for signing.

## Reconnection Features

The client includes robust reconnection logic:

- **Initial Connection**: Retries up to 60 times with exponential backoff (2×attempt seconds)
- **Authentication**: Retries up to 60 times with exponential backoff
- **Stream Reconnection**: Retries up to 5 times with 5-second delays
- **Smart Token Handling**: Only re-authenticates on auth errors, not connection errors
- **Channel Recreation**: Properly recreates gRPC channels on reconnection
- **Parallel Tasks**: Bundle and packet subscriptions run independently

## Protocol Support

The project implements the following Jito Block Engine protocol services:

### Server Services
- `BlockEngineValidator`: Provides bundle and packet subscriptions, fee info
- `AuthService`: Handles authentication challenges and token generation

### Client Features
- gRPC client connections with authentication
- Bundle hash calculation and verification (16-character hex hash)
- Packet counting and logging
- Connection state management
- Metadata handling for authentication headers

## Development

### Building

```bash
# Debug build (both binaries)
cargo build

# Release build
cargo build --release

# Build specific binary
cargo build --bin bundlesvr
cargo build --bin bundle_client
```

### Running with Debug Logs

```bash
RUST_LOG=debug cargo run --bin bundlesvr
RUST_LOG=debug cargo run --bin bundle_client
```

### Project Structure

```
src/
├── bin/
│   ├── bundlesvr.rs      # Server implementation
│   └── bundle_client.rs  # Client implementation
├── utils/
│   ├── auth.rs           # Authentication service
│   ├── keypair_manager.rs # Keypair management utilities
│   └── solana_blockhash_fetcher.rs # Blockhash fetching from Solana RPC
└── lib.rs                # Library exports

protos/                   # Protocol buffer definitions
├── auth.proto
├── block_engine.proto
├── bundle.proto
├── packet.proto
├── relayer.proto
└── shared.proto
```

### Key Features Implemented

- **Streaming gRPC**: Both bundle and packet subscriptions use streaming responses
- **Authentication**: Role-based authentication with Ed25519 keypair signing
- **Error Handling**: Comprehensive error handling with proper reconnection logic
- **Configurable Generation**: Adjustable intervals for bundle and packet generation
- **Bundle Verification**: Deterministic hash calculation for bundle integrity
- **Connection Management**: Automatic reconnection with exponential backoff
- **Concurrent Operations**: Client supports running multiple subscriptions in parallel

## Troubleshooting

### Common Issues

1. **Connection Refused**: Ensure the server is running before starting the client
2. **Authentication Errors**: Check that both server and client use the same keypair file
3. **Port Conflicts**: Verify no other services are using port 31001
4. **Keypair Missing**: Create a keypair file using `solana-keygen new --outfile id.json`

### Logs

Enable debug logging for detailed information:
```bash
RUST_LOG=debug cargo run --bin bundlesvr
RUST_LOG=debug cargo run --bin bundle_client
```
