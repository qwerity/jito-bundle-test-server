use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::proto::{
    bundle::Bundle,
    packet::PacketBatch
};

/// Calculate a deterministic hash for the bundle for verification purposes
pub fn calculate_bundle_hash(bundle: &Bundle) -> String {
    let mut hasher = DefaultHasher::new();

    bundle.packets.len().hash(&mut hasher);

    for (index, packet) in bundle.packets.iter().enumerate() {
        index.hash(&mut hasher);
        packet.data.hash(&mut hasher);

        if let Some(meta) = &packet.meta {
            meta.size.hash(&mut hasher);
            meta.addr.hash(&mut hasher);
            meta.port.hash(&mut hasher);
        }
    }

    if let Some(header) = &bundle.header {
        if let Some(ts) = &header.ts {
            ts.seconds.hash(&mut hasher);
            ts.nanos.hash(&mut hasher);
        }
    }

    format!("{:016x}", hasher.finish())
}

/// Calculate a deterministic hash for the packet batch for verification purposes
pub fn calculate_packet_batch_hash(batch: &PacketBatch) -> String {
    let mut hasher = DefaultHasher::new();

    batch.packets.len().hash(&mut hasher);

    for (index, packet) in batch.packets.iter().enumerate() {
        index.hash(&mut hasher);
        packet.data.hash(&mut hasher);

        if let Some(meta) = &packet.meta {
            meta.size.hash(&mut hasher);
            meta.addr.hash(&mut hasher);
            meta.port.hash(&mut hasher);
        }
    }

    format!("{:016x}", hasher.finish())
}