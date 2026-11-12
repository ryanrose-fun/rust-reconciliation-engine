use crate::Payment;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SourceReference {
    pub label: Option<String>,
    pub record_count: usize,
    /// SHA-256 over normalized compact JSON in input order, not raw uploaded bytes.
    pub sha256: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ReportMetadata {
    pub report_id: String,
    pub generated_at: String,
    pub engine_version: String,
    pub rules_version: String,
    pub fingerprint_format: String,
    pub internal_source: SourceReference,
    pub provider_source: SourceReference,
}

impl ReportMetadata {
    pub(crate) fn new() -> Self {
        Self {
            report_id: uuid::Uuid::new_v4().to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            rules_version: "2".into(),
            fingerprint_format: "payment-json-v1-input-order".into(),
            internal_source: source_reference(&[]),
            provider_source: source_reference(&[]),
        }
    }
}

pub(crate) fn source_reference(payments: &[Payment]) -> SourceReference {
    // These types contain no maps or fallible custom serializers.
    // Buffer serializer fragments before hashing, avoiding a full source-sized
    // JSON allocation while preserving the exact fingerprint byte format.
    use std::io::Write;
    struct HashWriter<'a>(&'a mut Sha256);
    impl Write for HashWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hash = Sha256::new();
    {
        let mut writer = std::io::BufWriter::with_capacity(8192, HashWriter(&mut hash));
        serde_json::to_writer(&mut writer, payments).expect("Payments serialize to JSON");
        writer.flush().expect("Hash writer is infallible");
    }
    SourceReference {
        label: None,
        record_count: payments.len(),
        sha256: format!("{:x}", hash.finalize()),
    }
}
