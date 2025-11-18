use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::proto::{
    bundle::Bundle,
    packet::PacketBatch,
    packet::Packet,
};

fn update_packets_hash(packets: &[Packet], hasher: &mut DefaultHasher) {
    for (index, packet) in packets.iter().enumerate() {
        index.hash(hasher);
        packet.data.hash(hasher);

        if let Some(meta) = &packet.meta {
            meta.size.hash(hasher);
            meta.addr.hash(hasher);
            meta.port.hash(hasher);
        }
    }
}

/// Calculate a deterministic hash for the bundle for verification purposes
pub fn calculate_bundle_hash(bundle: &Bundle) -> String {
    let mut hasher = DefaultHasher::new();

    bundle.packets.len().hash(&mut hasher);

    update_packets_hash(&bundle.packets, &mut hasher);

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

    update_packets_hash(&batch.packets, &mut hasher);

    format!("{:016x}", hasher.finish())
}
