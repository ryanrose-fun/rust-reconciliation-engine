//! Stateful GPU experiment. No CPU performance benchmark and no production persistence.
use super::{Gpu, Pod, Zeroable, stats};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, time::Instant};

#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
struct Value {
    vlo: u32,
    vhi: u32,
    low: u32,
    high: u32,
    currency: u32,
    status: u32,
}
impl Value {
    fn new(version: u64, amount: u64, currency: u32, status: u32) -> Self {
        Self {
            vlo: version as u32,
            vhi: (version >> 32) as u32,
            low: amount as u32,
            high: (amount >> 32) as u32,
            currency,
            status,
        }
    }
}
#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
struct Patch {
    id: u32,
    mask: u32,
    internal: Value,
    provider: Value,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub id: String,
    pub source: u32,
    pub version: u64,
    pub amount: u64,
    pub currency: u32,
    pub status: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Receipt {
    pub id: String,
    pub internal: u32,
    pub provider: u32,
    pub result: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Change {
    pub id: String,
    pub before: u32,
    pub after: u32,
}
pub struct Response {
    pub receipts: Vec<Receipt>,
    pub changes: Vec<Change>,
    pub ms: f64,
    pub upload_bytes: usize,
    pub download_bytes: usize,
}
#[derive(Debug, serde::Serialize)]
pub struct SourceState {
    pub version: u64,
    pub amount_minor: u64,
    pub currency: u32,
    pub status: u32,
}
#[derive(Debug, serde::Serialize)]
pub struct Snapshot {
    pub payment_id: String,
    pub internal: Option<SourceState>,
    pub provider: Option<SourceState>,
    pub result: u32,
}
pub struct Worker<'a> {
    gpu: &'a Gpu,
    buffers: Vec<wgpu::Buffer>,
    params: wgpu::Buffer,
    receipt_read: wgpu::Buffer,
    change_read: wgpu::Buffer,
    group: wgpu::BindGroup,
    pipeline: wgpu::ComputePipeline,
    dictionary: HashMap<String, u32>,
    ids: Vec<String>,
    capacity: usize,
    max_batch: usize,
}
impl<'a> Worker<'a> {
    pub fn snapshot(&self, id: &str) -> Option<Snapshot> {
        let slot = *self.dictionary.get(id)? as u64;
        let buffer = self.gpu.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("GPU payment query"),
            size: 52,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let mut encoder = self.gpu.device.create_command_encoder(&Default::default());
        encoder.copy_buffer_to_buffer(&self.buffers[0], slot * 24, &buffer, 0, 24);
        encoder.copy_buffer_to_buffer(&self.buffers[1], slot * 24, &buffer, 24, 24);
        encoder.copy_buffer_to_buffer(&self.buffers[2], slot * 4, &buffer, 48, 4);
        self.gpu.queue.submit(Some(encoder.finish()));
        let words = self.read(&buffer, 52);
        let decode = |v: &[u32]| {
            let version = v[0] as u64 | ((v[1] as u64) << 32);
            (version != 0).then(|| SourceState {
                version,
                amount_minor: v[2] as u64 | ((v[3] as u64) << 32),
                currency: v[4],
                status: v[5],
            })
        };
        Some(Snapshot {
            payment_id: id.into(),
            internal: decode(&words[..6]),
            provider: decode(&words[6..12]),
            result: words[12],
        })
    }
    pub fn new(gpu: &'a Gpu, capacity: usize, max_batch: usize) -> Self {
        assert!(capacity > 0 && max_batch > 0 && capacity <= 2_000_000 && max_batch <= 65_536);
        let d = &gpu.device;
        let make = |size, usage| {
            d.create_buffer(&wgpu::BufferDescriptor {
                label: None,
                size,
                usage,
                mapped_at_creation: false,
            })
        };
        let rw = wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_DST
            | wgpu::BufferUsages::COPY_SRC;
        let sizes = [
            capacity * 24,
            capacity * 24,
            capacity * 4,
            max_batch * 56,
            max_batch * 16,
            max_batch * 16,
            4,
        ];
        let buffers: Vec<_> = sizes.into_iter().map(|n| make(n as u64, rw)).collect();
        let params = make(
            16,
            wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        );
        let receipt_read = make(
            (max_batch * 16 + 4) as u64,
            wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        );
        let change_read = make(
            (max_batch * 16) as u64,
            wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        );
        let mut entries: Vec<_> = (0..7)
            .map(|binding| wgpu::BindGroupLayoutEntry {
                binding,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage {
                        read_only: binding == 3,
                    },
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            })
            .collect();
        entries.push(wgpu::BindGroupLayoutEntry {
            binding: 7,
            visibility: wgpu::ShaderStages::COMPUTE,
            ty: wgpu::BindingType::Buffer {
                ty: wgpu::BufferBindingType::Uniform,
                has_dynamic_offset: false,
                min_binding_size: None,
            },
            count: None,
        });
        let layout = d.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: None,
            entries: &entries,
        });
        let pl = d.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: None,
            bind_group_layouts: &[&layout],
            push_constant_ranges: &[],
        });
        let shader = d.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("resident versioned reconciliation"),
            source: wgpu::ShaderSource::Wgsl(include_str!("resident.wgsl").into()),
        });
        let pipeline = d.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: None,
            layout: Some(&pl),
            module: &shader,
            entry_point: Some("update"),
            compilation_options: Default::default(),
            cache: None,
        });
        let mut bindings: Vec<_> = buffers
            .iter()
            .enumerate()
            .map(|(i, b)| wgpu::BindGroupEntry {
                binding: i as u32,
                resource: b.as_entire_binding(),
            })
            .collect();
        bindings.push(wgpu::BindGroupEntry {
            binding: 7,
            resource: params.as_entire_binding(),
        });
        let group = d.create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &layout,
            entries: &bindings,
        });
        Self {
            gpu,
            buffers,
            params,
            receipt_read,
            change_read,
            group,
            pipeline,
            dictionary: HashMap::with_capacity(capacity),
            ids: Vec::with_capacity(capacity),
            capacity,
            max_batch,
        }
    }
    fn read(&self, buffer: &wgpu::Buffer, bytes: usize) -> Vec<u32> {
        let slice = buffer.slice(..bytes as u64);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        slice.map_async(wgpu::MapMode::Read, move |r| {
            tx.send(r).unwrap();
        });
        self.gpu.device.poll(wgpu::Maintain::Wait);
        rx.recv().unwrap().expect("GPU readback failed");
        let view = slice.get_mapped_range();
        let words = bytemuck::cast_slice(&view).to_vec();
        drop(view);
        buffer.unmap();
        words
    }
    // Mutable receiver serializes batches. Validation rejects malformed/duplicate batches
    // before assigning new IDs or writing GPU state. Business outcomes are per source.
    pub fn submit(&mut self, events: &[Event]) -> Result<Response, String> {
        let start = Instant::now();
        if events.len() > 2 * self.max_batch {
            return Err("event batch capacity exceeded".into());
        }
        let mut fresh: HashMap<&str, u32> = HashMap::new();
        let mut new_ids = Vec::new();
        let mut positions = HashMap::new();
        let mut patches: Vec<Patch> = Vec::new();
        for e in events {
            if e.id.trim().is_empty()
                || e.source > 1
                || e.version == 0
                || e.currency > 3
                || e.status > 2
            {
                return Err("invalid event".into());
            }
            let id = if let Some(&id) = self.dictionary.get(&e.id) {
                id
            } else if let Some(&id) = fresh.get(e.id.as_str()) {
                id
            } else {
                if self.ids.len() + new_ids.len() >= self.capacity {
                    return Err("resident payment capacity exceeded".into());
                }
                let id = (self.ids.len() + new_ids.len()) as u32;
                fresh.insert(&e.id, id);
                new_ids.push(e.id.clone());
                id
            };
            let i = *positions.entry(id).or_insert_with(|| {
                let i = patches.len();
                patches.push(Patch {
                    id,
                    ..Default::default()
                });
                i
            });
            if patches.len() > self.max_batch {
                return Err("distinct payment batch capacity exceeded".into());
            }
            let p = &mut patches[i];
            let bit = 1 << e.source;
            if p.mask & bit != 0 {
                return Err("multiple updates for the same source and ID in one batch".into());
            }
            p.mask |= bit;
            let value = Value::new(e.version, e.amount, e.currency, e.status);
            if e.source == 0 {
                p.internal = value;
            } else {
                p.provider = value;
            }
        }
        for id in new_ids {
            self.dictionary.insert(id.clone(), self.ids.len() as u32);
            self.ids.push(id);
        }
        if patches.is_empty() {
            return Ok(Response {
                receipts: vec![],
                changes: vec![],
                ms: start.elapsed().as_secs_f64() * 1000.0,
                upload_bytes: 0,
                download_bytes: 0,
            });
        }
        let n = patches.len();
        let queue = &self.gpu.queue;
        queue.write_buffer(&self.buffers[3], 0, bytemuck::cast_slice(&patches));
        queue.write_buffer(&self.params, 0, bytemuck::cast_slice(&[n as u32, 0, 0, 0]));
        let mut encoder = self.gpu.device.create_command_encoder(&Default::default());
        encoder.clear_buffer(&self.buffers[6], 0, None);
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: None,
                timestamp_writes: None,
            });
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.group, &[]);
            pass.dispatch_workgroups((n as u32).div_ceil(256), 1, 1);
        }
        encoder.copy_buffer_to_buffer(&self.buffers[4], 0, &self.receipt_read, 0, (n * 16) as u64);
        encoder.copy_buffer_to_buffer(&self.buffers[6], 0, &self.receipt_read, (n * 16) as u64, 4);
        queue.submit(Some(encoder.finish()));
        let words = self.read(&self.receipt_read, n * 16 + 4);
        let changed = words[n * 4] as usize;
        assert!(changed <= n);
        let receipts = words[..n * 4]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|r| Receipt {
                id: self.ids[r[0] as usize].clone(),
                internal: r[1],
                provider: r[2],
                result: r[3],
            })
            .collect();
        // Read precisely the compact changed rows; unchanged state never comes back.
        let changes = if changed == 0 {
            vec![]
        } else {
            let mut encoder = self.gpu.device.create_command_encoder(&Default::default());
            encoder.copy_buffer_to_buffer(
                &self.buffers[5],
                0,
                &self.change_read,
                0,
                (changed * 16) as u64,
            );
            queue.submit(Some(encoder.finish()));
            self.read(&self.change_read, changed * 16)
                .as_chunks::<4>()
                .0
                .iter()
                .map(|r| Change {
                    id: self.ids[r[0] as usize].clone(),
                    before: r[1],
                    after: r[2],
                })
                .collect()
        };
        drop(patches);
        drop(positions);
        drop(fresh);
        drop(words);
        Ok(Response {
            receipts,
            changes,
            ms: start.elapsed().as_secs_f64() * 1000.0,
            upload_bytes: n * 56 + 16,
            download_bytes: n * 16 + 4 + changed * 16,
        })
    }
}
fn event(id: &str, source: u32, version: u64, amount: u64, status: u32) -> Event {
    Event {
        id: id.into(),
        source,
        version,
        amount,
        currency: 0,
        status,
    }
}
fn checks(gpu: &Gpu) {
    let mut w = Worker::new(gpu, 16, 8);
    let r = w.submit(&[event("x", 0, 1, u64::MAX, 1)]).unwrap();
    assert_eq!(r.receipts[0].result, 16);
    assert_eq!(r.changes[0].before, 0);
    let r = w.submit(&[event("x", 1, 1, u64::MAX, 1)]).unwrap();
    assert_eq!(r.receipts[0].result, 1);
    assert_eq!(r.changes[0].before, 16);
    let r = w.submit(&[event("x", 1, 1, u64::MAX, 1)]).unwrap();
    assert_eq!(r.receipts[0].provider, 2);
    assert!(r.changes.is_empty());
    let r = w.submit(&[event("x", 1, 1, 0, 0)]).unwrap();
    assert_eq!(r.receipts[0].provider, 3);
    assert_eq!(r.receipts[0].result, 1);
    assert!(r.changes.is_empty());
    let r = w.submit(&[event("x", 1, 1u64 << 32, 0, 0)]).unwrap();
    assert_eq!(r.receipts[0].result, 11);
    let r = w
        .submit(&[event("x", 1, u32::MAX as u64, u64::MAX, 1)])
        .unwrap();
    assert_eq!(r.receipts[0].provider, 4);
    assert_eq!(r.receipts[0].result, 11);
    assert!(r.changes.is_empty());
    let r = w.submit(&[event("x", 1, u64::MAX, u64::MAX, 1)]).unwrap();
    assert_eq!(r.receipts[0].result, 1);
    assert!(
        w.submit(&[event("z", 0, 1, 1, 1), event("z", 0, 2, 2, 1)])
            .is_err()
    );
    assert!(!w.dictionary.contains_key("z"));
    assert!(w.submit(&[event("z", 0, 0, 1, 1)]).is_err());
    assert!(!w.dictionary.contains_key("z"));
    for mask in 0..8 {
        let id = format!("mask-{mask}");
        let a = event(&id, 0, 1, u64::MAX, 1);
        let mut b = event(
            &id,
            1,
            1,
            u64::MAX - u64::from(mask & 1 != 0),
            if mask & 4 != 0 { 2 } else { 1 },
        );
        b.currency = u32::from(mask & 2 != 0);
        let r = w.submit(&[a, b]).unwrap();
        assert_eq!(r.receipts[0].result, 1 | (mask << 1));
    }
    let r = w.submit(&[event("only-provider", 1, 1, 0, 0)]).unwrap();
    assert_eq!(r.receipts[0].result, 32);
    let r = w.submit(&[event("mask-0", 1, 2, u64::MAX, 1)]).unwrap();
    assert_eq!(r.receipts[0].provider, 1);
    assert!(r.changes.is_empty()); // New evidence version, same reconciliation result.
    let r = w
        .submit(&[event("mask-0", 0, 2, 0, 1), event("mask-0", 1, 2, 0, 0)])
        .unwrap();
    assert_eq!(
        (
            r.receipts[0].internal,
            r.receipts[0].provider,
            r.receipts[0].result
        ),
        (1, 3, 3)
    );
    let mut tiny = Worker::new(gpu, 1, 1);
    assert!(
        tiny.submit(&[event("a", 0, 1, 0, 0), event("b", 1, 1, 0, 0)])
            .is_err()
    );
    assert!(tiny.ids.is_empty());
    assert!(tiny.submit(&[]).unwrap().receipts.is_empty());
    let mut bounded = Worker::new(gpu, 16, 1);
    assert!(
        bounded
            .submit(&[event("a", 0, 1, 0, 0), event("b", 1, 1, 0, 0)])
            .is_err()
    );
    assert!(bounded.ids.is_empty());
    println!(
        "Resident GPU checks passed: retries, conflicts, stale/u64 versions, all mismatch combinations, missing sources, duplicate batches, limits and empty batches"
    );
}
pub fn run(n: usize, path: &str) {
    let start = Instant::now();
    let gpu = Gpu::new();
    let mut worker = Worker::new(&gpu, n, 65_536);
    let initialization_ms = start.elapsed().as_secs_f64() * 1000.0;
    checks(&gpu);
    let start = Instant::now();
    let mut seed_submit_ms = 0.0;
    for begin in (0..n).step_by(65_536) {
        let end = (begin + 65_536).min(n);
        let mut events = Vec::with_capacity((end - begin) * 2);
        for i in begin..end {
            let id = format!("LIVE-{i:010}");
            events.push(event(&id, 0, 1, 10000, 1));
            events.push(event(&id, 1, 1, 10000, 1));
        }
        let r = worker.submit(&events).unwrap();
        seed_submit_ms += r.ms;
        assert_eq!(r.receipts.len(), end - begin);
        assert_eq!(r.changes.len(), end - begin);
        for (offset, receipt) in r.receipts.iter().enumerate() {
            assert_eq!(receipt.id, events[2 * offset].id);
            assert_eq!(
                (receipt.internal, receipt.provider, receipt.result),
                (1, 1, 1)
            );
        }
        assert!(r.changes.iter().all(|c| c.before == 0 && c.after == 1));
    }
    let initial_load_wall_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!(
        "Resident state loaded: {n} payments, {} source values; {:.1} ms submit time",
        2 * n,
        seed_submit_ms
    );
    let mut version = 2u64;
    let mut results = Vec::new();
    let mut updated_prefix = 0usize;
    for count in [1usize, 100, 1000, 10000, 65536]
        .into_iter()
        .filter(|&c| c <= n)
    {
        updated_prefix = count;
        let mut times = Vec::new();
        let mut replay_times = Vec::new();
        let mut bytes = (0, 0, 0);
        for round in 0..22 {
            let pending = round % 2 == 0;
            let code = if pending { 11 } else { 1 };
            let before = if pending { 1 } else { 11 };
            let events: Vec<_> = (0..count)
                .map(|i| {
                    event(
                        &format!("LIVE-{i:010}"),
                        1,
                        version,
                        if pending { 9500 } else { 10000 },
                        if pending { 0 } else { 1 },
                    )
                })
                .collect();
            version += 1;
            let r = worker.submit(&events).unwrap();
            assert_eq!(r.receipts.len(), count);
            assert_eq!(r.changes.len(), count);
            for (receipt, e) in r.receipts.iter().zip(&events) {
                assert_eq!(receipt.id, e.id);
                assert_eq!(
                    (receipt.internal, receipt.provider, receipt.result),
                    (0, 1, code)
                );
            }
            let mut changed_ids: Vec<_> = r
                .changes
                .iter()
                .map(|c| {
                    assert_eq!((c.before, c.after), (before, code));
                    c.id.as_str()
                })
                .collect();
            changed_ids.sort_unstable();
            assert!(changed_ids.iter().zip(&events).all(|(id, e)| *id == e.id));
            let retry = worker.submit(&events).unwrap();
            assert_eq!(retry.receipts.len(), count);
            assert!(retry.changes.is_empty());
            assert!(
                retry
                    .receipts
                    .iter()
                    .zip(&events)
                    .all(|(receipt, e)| receipt.id == e.id
                        && receipt.provider == 2
                        && receipt.result == code)
            );
            if round >= 2 {
                times.push(r.ms);
                replay_times.push(retry.ms);
            }
            bytes = (r.upload_bytes, r.download_bytes, retry.download_bytes);
        }
        println!(
            "{count} updates: median {:.3} ms; exact retry {:.3} ms; all receipts and changes verified",
            stats(&times)["median_ms"].as_f64().unwrap(),
            stats(&replay_times)["median_ms"].as_f64().unwrap()
        );
        results.push(json!({"updates_per_batch":count,"updated":stats(&times),"exact_retry":stats(&replay_times),"upload_bytes":bytes.0,"download_bytes_changed":bytes.1,"download_bytes_retry":bytes.2}));
    }
    // Verify every resident payment after incremental work, including untouched IDs.
    // Original internal records must be exact retries; updated provider records stale.
    // This GPU audit is outside performance samples and must not change any result.
    for begin in (0..n).step_by(65_536) {
        let end = (begin + 65_536).min(n);
        let mut events = Vec::with_capacity((end - begin) * 2);
        for i in begin..end {
            let id = format!("LIVE-{i:010}");
            events.push(event(&id, 0, 1, 10000, 1));
            events.push(event(&id, 1, 1, 10000, 1));
        }
        let r = worker.submit(&events).unwrap();
        assert_eq!(r.receipts.len(), end - begin);
        assert!(r.changes.is_empty());
        for (offset, receipt) in r.receipts.iter().enumerate() {
            assert_eq!(receipt.id, events[2 * offset].id);
            assert_eq!(
                (receipt.internal, receipt.provider, receipt.result),
                (
                    2,
                    if begin + offset < updated_prefix {
                        4
                    } else {
                        2
                    },
                    1
                )
            );
        }
    }
    println!("Final GPU audit passed for all {n} resident payments, including untouched records");
    let report = json!({"schema":"resident-gpu-v1","adapter":gpu.info.name,"backend":format!("{:?}",gpu.info.backend),"resident_payments":n,"resident_source_values":2*n,"capacity":n,"max_batch_distinct_ids":65536,
        "initialization_ms":initialization_ms,"initial_load_submit_ms":seed_submit_ms,"initial_load_wall_ms":initial_load_wall_ms,"results":results,"all_gpu_checks_and_outputs_passed":true,"final_resident_audit_payments":n,"cpu_benchmarks_run":false,
        "boundary":"Persistent GPU state. Timed submit includes exact string-ID lookup, validation/coalescing, packing, upload, GPU version checks/state update/comparison/changed-result compaction, receipt and changed-result readback, owned output ID creation and scratch cleanup. Batch fixture creation and validation excluded. State bootstrap and device creation separate. No HTTP, JSON, durable storage, event-ID deduplication, concurrent submissions, restart recovery or multi-GPU.",
        "semantics":"Version is nonzero u64 per payment/source; equal version+payload is duplicate; equal version with different payload conflicts; lower version is stale. One update per payment/source per batch; two sources can coalesce. Each source applies independently; no atomic whole-batch transaction. Changes mean reconciliation result-code changes, not every evidence change. No missing-source waiting period.",
        "source_sha256":format!("{:x}",Sha256::digest(include_bytes!("resident.rs"))),"shader_sha256":format!("{:x}",Sha256::digest(include_bytes!("resident.wgsl")))});
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!("Saved {path}");
}
