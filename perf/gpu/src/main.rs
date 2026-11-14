use bytemuck::{Pod, Zeroable};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, time::Instant};

mod comparison;
pub mod resident;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
struct Payment {
    id: u32,
    low: u32,
    high: u32,
    currency: u32,
    status: u32,
}
impl Payment {
    fn new(id: u32, amount: u64, currency: u32, status: u32) -> Self {
        Self {
            id,
            low: amount as u32,
            high: (amount >> 32) as u32,
            currency,
            status,
        }
    }
}

pub struct Gpu {
    device: wgpu::Device,
    queue: wgpu::Queue,
    layout: wgpu::BindGroupLayout,
    scatter: wgpu::ComputePipeline,
    compare: wgpu::ComputePipeline,
    info: wgpu::AdapterInfo,
}
impl Gpu {
    fn new() -> Self {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::DX12,
            ..Default::default()
        });
        let adapter = instance
            .enumerate_adapters(wgpu::Backends::DX12)
            .into_iter()
            .find(|a| {
                let i = a.get_info();
                i.vendor == 0x10de
                    && i.name.contains("5070")
                    && i.device_type == wgpu::DeviceType::DiscreteGpu
            })
            .expect("RTX 5070 hardware adapter required; refusing a CPU/software fallback");
        let info = adapter.get_info();
        println!(
            "Adapter: {} ({:?}), driver {} {}",
            info.name, info.backend, info.driver, info.driver_info
        );
        let (device, queue) = pollster::block_on(adapter.request_device(
            &wgpu::DeviceDescriptor {
                label: Some("bulk prototype"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                memory_hints: wgpu::MemoryHints::Performance,
            },
            None,
        ))
        .expect("GPU device initialization");
        let entries: Vec<_> = (0..6)
            .map(|binding| wgpu::BindGroupLayoutEntry {
                binding,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage {
                        read_only: binding < 2,
                    },
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            })
            .collect();
        let layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: None,
            entries: &entries,
        });
        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: None,
            bind_group_layouts: &[&layout],
            push_constant_ranges: &[],
        });
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("exact integer reconciliation"),
            source: wgpu::ShaderSource::Wgsl(include_str!("reconcile.wgsl").into()),
        });
        let pipeline = |entry| {
            device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some(entry),
                layout: Some(&pipeline_layout),
                module: &module,
                entry_point: Some(entry),
                compilation_options: Default::default(),
                cache: None,
            })
        };
        let scatter = pipeline("scatter");
        let compare = pipeline("compare");
        Self {
            device,
            queue,
            layout,
            scatter,
            compare,
            info,
        }
    }
    fn buffers<'a>(&'a self, internal: &[Payment], provider: &[Payment], ids: usize) -> Batch<'a> {
        assert!(
            !internal.is_empty() && !provider.is_empty() && ids > 0,
            "prototype requires nonempty snapshots"
        );
        let make = |size, usage| {
            self.device.create_buffer(&wgpu::BufferDescriptor {
                label: None,
                size,
                usage,
                mapped_at_creation: false,
            })
        };
        let storage = wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST;
        let a = make(std::mem::size_of_val(internal) as u64, storage);
        let b = make(std::mem::size_of_val(provider) as u64, storage);
        let ii = make(ids as u64 * 4, storage);
        let pi = make(ids as u64 * 4, storage);
        let out = make(ids as u64 * 4, storage | wgpu::BufferUsages::COPY_SRC);
        let errors = make(12, storage | wgpu::BufferUsages::COPY_SRC);
        let readback = make(
            ids as u64 * 4 + 12,
            wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
        );
        let buffers = [a, b, ii, pi, out, errors];
        let entries: Vec<_> = buffers
            .iter()
            .enumerate()
            .map(|(i, b)| wgpu::BindGroupEntry {
                binding: i as u32,
                resource: b.as_entire_binding(),
            })
            .collect();
        let group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &self.layout,
            entries: &entries,
        });
        Batch {
            gpu: self,
            buffers,
            group,
            readback,
            ids,
            rows: internal.len().max(provider.len()),
        }
    }
}
struct Batch<'a> {
    gpu: &'a Gpu,
    buffers: [wgpu::Buffer; 6],
    group: wgpu::BindGroup,
    readback: wgpu::Buffer,
    ids: usize,
    rows: usize,
}
impl Batch<'_> {
    // Wall clock includes submission, clear, both kernels, output transfer and map wait.
    // Upload mode additionally includes queue.write_buffer and input transfers.
    fn run(&self, upload: Option<(&[Payment], &[Payment])>) -> (Vec<u32>, [u32; 3], f64) {
        let start = Instant::now();
        if let Some((a, b)) = upload {
            self.gpu
                .queue
                .write_buffer(&self.buffers[0], 0, bytemuck::cast_slice(a));
            self.gpu
                .queue
                .write_buffer(&self.buffers[1], 0, bytemuck::cast_slice(b));
        }
        let mut encoder = self.gpu.device.create_command_encoder(&Default::default());
        for i in [2, 3, 5] {
            encoder.clear_buffer(&self.buffers[i], 0, None);
        }
        for (pipeline, count) in [
            (&self.gpu.scatter, self.rows),
            (&self.gpu.compare, self.ids),
        ] {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: None,
                timestamp_writes: None,
            });
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &self.group, &[]);
            pass.dispatch_workgroups((count as u32).div_ceil(256), 1, 1);
        }
        encoder.copy_buffer_to_buffer(&self.buffers[4], 0, &self.readback, 0, self.ids as u64 * 4);
        encoder.copy_buffer_to_buffer(&self.buffers[5], 0, &self.readback, self.ids as u64 * 4, 12);
        self.gpu.queue.submit(Some(encoder.finish()));
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.readback
            .slice(..)
            .map_async(wgpu::MapMode::Read, move |r| {
                tx.send(r).unwrap();
            });
        self.gpu.device.poll(wgpu::Maintain::Wait);
        rx.recv().unwrap().expect("readback map");
        let mapped = self.readback.slice(..).get_mapped_range();
        let values: &[u32] = bytemuck::cast_slice(&mapped);
        let result = values[..self.ids].to_vec();
        let errors = values[self.ids..].try_into().unwrap();
        drop(mapped);
        self.readback.unmap();
        (result, errors, start.elapsed().as_secs_f64() * 1000.0)
    }
}

// Untimed independent correctness oracle only; NOT a CPU performance benchmark.
fn expected(a: &[Payment], b: &[Payment], ids: usize) -> Vec<u32> {
    let a: HashMap<_, _> = a.iter().map(|p| (p.id, p)).collect();
    let b: HashMap<_, _> = b.iter().map(|p| (p.id, p)).collect();
    (0..ids as u32)
        .map(|id| match (a.get(&id), b.get(&id)) {
            (None, None) => 0,
            (Some(_), None) => 16,
            (None, Some(_)) => 32,
            (Some(x), Some(y)) => {
                1 | (u32::from((x.low, x.high) != (y.low, y.high)) * 2)
                    | (u32::from(x.currency != y.currency) * 4)
                    | (u32::from(x.status != y.status) * 8)
            }
        })
        .collect()
}
fn correctness(gpu: &Gpu) {
    let a: Vec<_> = (0..8).map(|id| Payment::new(id, u64::MAX, 0, 1)).collect();
    let mut b: Vec<_> = (0..8)
        .map(|id| {
            Payment::new(
                id,
                if id & 1 != 0 {
                    u64::MAX - (1u64 << 32)
                } else {
                    u64::MAX
                },
                u32::from(id & 2 != 0),
                if id & 4 != 0 { 2 } else { 1 },
            )
        })
        .collect();
    b.reverse();
    b.push(Payment::new(9, 0, 3, 0));
    let mut a = a;
    a.push(Payment::new(8, 0, 3, 0));
    let (got, errors, _) = gpu.buffers(&a, &b, 11).run(Some((&a, &b)));
    assert_eq!(errors, [0, 0, 0]);
    assert_eq!(got, expected(&a, &b, 11));
    // Low word, rather than only high word, must also participate in equality.
    b.iter_mut().find(|p| p.id == 0).unwrap().low -= 1;
    let (got, errors, _) = gpu.buffers(&a, &b, 11).run(Some((&a, &b)));
    assert_eq!(errors, [0, 0, 0]);
    assert_eq!(got, expected(&a, &b, 11));
    a.push(a[0]);
    b.push(b[0]);
    let (_, errors, _) = gpu.buffers(&a, &b, 11).run(Some((&a, &b)));
    assert_eq!(errors, [1, 1, 0]); // Caller must discard ALL results when errors != 0.
    a[0].id = 99;
    let (_, errors, _) = gpu.buffers(&a, &b, 11).run(Some((&a, &b)));
    assert_eq!(errors[2], 1);
    println!(
        "GPU correctness: all mismatch combinations, missing sources, shuffled IDs, u64 boundaries, duplicate and range rejection passed"
    );
}
fn stats(values: &[f64]) -> serde_json::Value {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let median = (v[(v.len() - 1) / 2] + v[v.len() / 2]) / 2.0;
    json!({"samples_ms":values,"min_ms":v[0],"median_ms":median,"max_ms":v[v.len()-1]})
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let n: usize = args
        .get(1)
        .map(|s| s.parse().expect("payment count"))
        .unwrap_or(1_000_000);
    let path = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("gpu-bulk-results.json");
    assert!(
        (100..=2_000_000).contains(&n),
        "supported prototype size: 100..=2000000 per source"
    );
    if args.get(3).is_some_and(|arg| arg == "--resident") {
        resident::run(n, path);
        return;
    }
    if args.get(3).is_some_and(|arg| arg == "--multicore") {
        comparison::multicore::run(n, path);
        return;
    }
    if args.get(3).is_some_and(|arg| arg == "--compare") {
        comparison::run(n, path);
        return;
    }
    let cold = Instant::now();
    let gpu = Gpu::new();
    let initialization_ms = cold.elapsed().as_secs_f64() * 1000.0;
    correctness(&gpu);
    // Build realistic string IDs outside measured preparation. Provider order is reversed.
    let internal_ids: Vec<_> = (0..n).map(|i| format!("PAY-{i:010}")).collect();
    let provider_ids: Vec<_> = (0..n)
        .rev()
        .map(|i| format!("PAY-{:010}", if i % 100 == 0 { n + i } else { i }))
        .collect();
    let prepare = Instant::now();
    let mut dictionary: HashMap<String, u32> = HashMap::with_capacity(n + n / 100 + 1);
    let mut next = 0u32;
    let mut pack = |ids: &[String], provider: bool| -> Vec<Payment> {
        ids.iter()
            .enumerate()
            .map(|(position, id)| {
                let dense = if let Some(&v) = dictionary.get(id.as_str()) {
                    v
                } else {
                    let v = next;
                    next += 1;
                    dictionary.insert(id.clone(), v);
                    v
                };
                let original = if provider { n - 1 - position } else { position };
                // Every 16 IDs covers all eight combinations; the other half match.
                let mask = if provider && original % 16 < 8 {
                    (original % 8) as u32
                } else {
                    0
                };
                Payment::new(
                    dense,
                    u64::MAX - original as u64 - u64::from(mask & 1 != 0),
                    u32::from(mask & 2 != 0),
                    if mask & 4 != 0 { 0 } else { 1 },
                )
            })
            .collect()
    };
    let a = pack(&internal_ids, false);
    let b = pack(&provider_ids, true);
    let preparation_ms = prepare.elapsed().as_secs_f64() * 1000.0;
    let ids = next as usize;
    let expected = expected(&a, &b, ids);
    let allocation = Instant::now();
    let batch = gpu.buffers(&a, &b, ids);
    let allocation_ms = allocation.elapsed().as_secs_f64() * 1000.0;
    let (warm, errors, cold_batch_ms) = batch.run(Some((&a, &b)));
    assert_eq!(errors, [0, 0, 0]);
    assert_eq!(warm, expected);
    let mut uploaded = Vec::new();
    let mut resident = Vec::new();
    for i in 0..21 {
        let (out, err, ms) = batch.run(Some((&a, &b)));
        assert_eq!(err, [0, 0, 0]);
        assert_eq!(out, expected);
        uploaded.push(ms);
        let (out, err, ms) = batch.run(None);
        assert_eq!(err, [0, 0, 0]);
        assert_eq!(out, expected);
        resident.push(ms);
        println!(
            "Trial {}/21: upload + compute + readback {:.3} ms; resident + readback {:.3} ms; every result verified",
            i + 1,
            uploaded[i],
            resident[i]
        );
    }
    let matched = expected.iter().filter(|&&v| v == 1).count();
    let report = json!({"schema":"gpu-bulk-prototype-v1","adapter":{"name":gpu.info.name,"backend":format!("{:?}",gpu.info.backend),"driver":gpu.info.driver,"driver_info":gpu.info.driver_info,"vendor":gpu.info.vendor,"device":gpu.info.device},
        "payments_per_source":n,"input_records":2*n,"unique_payment_ids":ids,"matched":matched,"discrepancies":ids-matched,
        "initialization_ms":initialization_ms,"gpu_buffer_allocation_ms":allocation_ms,"host_string_id_normalization_and_packing_ms":preparation_ms,"cold_first_batch_ms":cold_batch_ms,
        "input_bytes":std::mem::size_of_val(a.as_slice())+std::mem::size_of_val(b.as_slice()),"readback_bytes":ids*4+12,
        "upload_compute_readback":stats(&uploaded),"resident_compute_readback":stats(&resident),"validated_all_outputs_all_trials":true,
        "result_sha256":format!("{:x}",Sha256::digest(bytemuck::cast_slice(&expected))),"shader_sha256":format!("{:x}",Sha256::digest(include_bytes!("reconcile.wgsl"))),
        "scope":"Dense ID join + exact u64 amount/currency/status comparison. Host exact-string normalization measured separately. No JSON, HTTP, report evidence materialization, storage, durability or history. Resident measurement includes readback, not pure kernel time. CPU oracle is correctness-only and untimed. No CPU benchmark rerun."});
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).expect("write report");
    println!(
        "Saved {path}; preparation {preparation_ms:.3} ms; {ids} unique IDs, {matched} matched"
    );
}
