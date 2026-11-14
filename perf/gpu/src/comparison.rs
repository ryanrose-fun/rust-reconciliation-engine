//! Equal-boundary comparison: typed string-ID snapshots -> owned (ID, result) rows.
//! All preparation, temporary allocation/destruction and GPU transfers are timed.
use super::{Gpu, Payment, stats};
pub(super) mod multicore;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hint::black_box,
    time::Instant,
};

#[derive(Clone)]
struct Input {
    id: String,
    amount: u64,
    currency: u32,
    status: u32,
}
type Output = (Vec<(String, u32)>, [u32; 3]);
struct Packed<'a> {
    internal: Vec<Payment>,
    provider: Vec<Payment>,
    ids: Vec<&'a str>,
}

fn prepare<'a>(a: &'a [Input], b: &'a [Input]) -> Packed<'a> {
    let mut index: HashMap<&str, u32> = HashMap::with_capacity(a.len().max(b.len()));
    let mut ids = Vec::with_capacity(a.len().max(b.len()));
    let mut pack = |source: &'a [Input]| {
        source
            .iter()
            .map(|p| {
                let id = *index.entry(&p.id).or_insert_with(|| {
                    let id = ids.len() as u32;
                    ids.push(p.id.as_str());
                    id
                });
                Payment::new(id, p.amount, p.currency, p.status)
            })
            .collect()
    };
    let internal = pack(a);
    let provider = pack(b);
    Packed {
        internal,
        provider,
        ids,
    }
}

fn classify(a: Option<&Input>, b: Option<&Input>) -> u32 {
    match (a, b) {
        (None, None) => 0,
        (Some(_), None) => 16,
        (None, Some(_)) => 32,
        (Some(x), Some(y)) => {
            1 | (u32::from(x.amount != y.amount) * 2)
                | (u32::from(x.currency != y.currency) * 4)
                | (u32::from(x.status != y.status) * 8)
        }
    }
}

// Native CPU path: borrow IDs and input values; no GPU representation required.
fn cpu_direct(a: &[Input], b: &[Input]) -> Output {
    type Pair<'a> = (&'a str, Option<&'a Input>, Option<&'a Input>);
    let mut index: HashMap<&str, usize> = HashMap::with_capacity(a.len().max(b.len()));
    let mut order: Vec<Pair<'_>> = Vec::with_capacity(a.len().max(b.len()));
    let mut errors = [0; 3];
    for (source, records) in [a, b].into_iter().enumerate() {
        for p in records {
            let position = *index.entry(p.id.as_str()).or_insert_with(|| {
                let i = order.len();
                order.push((p.id.as_str(), None, None));
                i
            });
            let pair = &mut order[position];
            let slot = if source == 0 {
                &mut pair.1
            } else {
                &mut pair.2
            };
            if slot.replace(p).is_some() {
                errors[source] += 1;
            }
        }
    }
    if errors != [0; 3] {
        return (Vec::new(), errors);
    }
    let rows = order
        .into_iter()
        .map(|(id, x, y)| (id.to_owned(), classify(x, y)))
        .collect();
    (rows, errors)
}

fn cpu_packed(p: &Packed<'_>) -> (Vec<u32>, [u32; 3]) {
    let mut ai = vec![0usize; p.ids.len()];
    let mut bi = vec![0usize; p.ids.len()];
    let mut errors = [0; 3];
    for (source, (rows, index)) in [(&p.internal, &mut ai), (&p.provider, &mut bi)]
        .into_iter()
        .enumerate()
    {
        for (i, row) in rows.iter().enumerate() {
            if index[row.id as usize] != 0 {
                errors[source] += 1;
            }
            index[row.id as usize] = i + 1;
        }
    }
    if errors != [0; 3] {
        return (Vec::new(), errors);
    }
    let out = ai
        .into_iter()
        .zip(bi)
        .map(|(a, b)| {
            if a == 0 {
                return if b == 0 { 0 } else { 32 };
            }
            if b == 0 {
                return 16;
            }
            let x = p.internal[a - 1];
            let y = p.provider[b - 1];
            1 | (u32::from((x.low, x.high) != (y.low, y.high)) * 2)
                | (u32::from(x.currency != y.currency) * 4)
                | (u32::from(x.status != y.status) * 8)
        })
        .collect();
    (out, errors)
}

// mode 0 = direct CPU, 1 = packed CPU, 2 = packed GPU.
// Returning from this function drops all scratch allocations before caller stops time.
fn execute(mode: usize, gpu: &Gpu, a: &[Input], b: &[Input]) -> Output {
    if mode == 0 {
        return cpu_direct(a, b);
    }
    let packed = prepare(a, b);
    let (codes, errors) = if mode == 1 {
        cpu_packed(&packed)
    } else {
        let batch = gpu.buffers(&packed.internal, &packed.provider, packed.ids.len());
        let (codes, errors, _) = batch.run(Some((&packed.internal, &packed.provider)));
        drop(batch);
        // Drain backend resource cleanup as well as waiting for readback.
        gpu.device.poll(wgpu::Maintain::Wait);
        (codes, errors)
    };
    if errors != [0; 3] {
        return (Vec::new(), errors);
    }
    let rows = packed
        .ids
        .iter()
        .zip(codes)
        .map(|(id, code)| ((*id).to_owned(), code))
        .collect();
    (rows, errors)
}

// Independently implemented sorted-map oracle, used outside performance intervals.
fn oracle(a: &[Input], b: &[Input]) -> Output {
    let mut ai = BTreeMap::new();
    let mut bi = BTreeMap::new();
    let mut errors = [0; 3];
    for p in a {
        if ai.insert(p.id.as_str(), p).is_some() {
            errors[0] += 1;
        }
    }
    for p in b {
        if bi.insert(p.id.as_str(), p).is_some() {
            errors[1] += 1;
        }
    }
    if errors != [0; 3] {
        return (vec![], errors);
    }
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for p in a.iter().chain(b) {
        if !seen.insert(p.id.as_str()) {
            continue;
        }
        let result = match (ai.get(p.id.as_str()), bi.get(p.id.as_str())) {
            (Some(x), Some(y)) => {
                let mut r = 1;
                if x.amount != y.amount {
                    r += 2;
                }
                if x.currency != y.currency {
                    r += 4;
                }
                if x.status != y.status {
                    r += 8;
                }
                r
            }
            (Some(_), None) => 16,
            (None, Some(_)) => 32,
            (None, None) => unreachable!(),
        };
        rows.push((p.id.clone(), result));
    }
    (rows, errors)
}

fn shuffle<T>(values: &mut [T], mut state: u64) {
    for i in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(i, (state % (i as u64 + 1)) as usize);
    }
}
fn fixture(n: usize) -> (Vec<Input>, Vec<Input>) {
    let mut a = Vec::with_capacity(n);
    let mut b = Vec::with_capacity(n);
    for i in 0..n {
        let amount = if i % 17 == 0 { 0 } else { u64::MAX - i as u64 };
        a.push(Input {
            id: format!("PAY-{i:010}"),
            amount,
            currency: 0,
            status: 1,
        });
        let mask = if i % 16 < 8 { i % 8 } else { 0 };
        b.push(Input {
            id: format!("PAY-{:010}", if i % 100 == 0 { n + i } else { i }),
            amount: amount ^ u64::from(mask & 1 != 0),
            currency: u32::from(mask & 2 != 0),
            status: if mask & 4 != 0 { 0 } else { 1 },
        });
    }
    shuffle(&mut a, 0x13ac_7429);
    shuffle(&mut b, 0xab63_491d);
    (a, b)
}
fn validate(gpu: &Gpu) {
    let (a, b) = fixture(101);
    for (a, b) in [
        (a.clone(), b.clone()),
        (
            {
                let mut x = a.clone();
                x.push(x[0].clone());
                x
            },
            b.clone(),
        ),
        (a.clone(), {
            let mut x = b.clone();
            x.push(x[0].clone());
            x
        }),
        (
            vec![Input {
                id: "max".into(),
                amount: u64::MAX,
                currency: 3,
                status: 2,
            }],
            vec![Input {
                id: "max".into(),
                amount: u64::MAX - (1 << 32),
                currency: 3,
                status: 2,
            }],
        ),
    ] {
        let want = oracle(&a, &b);
        for mode in 0..3 {
            assert_eq!(execute(mode, gpu, &a, &b), want, "correctness mode {mode}");
        }
    }
}
fn input_hash(a: &[Input], b: &[Input]) -> String {
    let mut h = Sha256::new();
    for source in [a, b] {
        h.update((source.len() as u64).to_le_bytes());
        for p in source {
            h.update((p.id.len() as u64).to_le_bytes());
            h.update(p.id.as_bytes());
            h.update(p.amount.to_le_bytes());
            h.update(p.currency.to_le_bytes());
            h.update(p.status.to_le_bytes());
        }
    }
    format!("{:x}", h.finalize())
}
pub(super) fn run(n: usize, path: &str) {
    let (a, b) = fixture(n);
    let want = oracle(&a, &b);
    let start = Instant::now();
    let gpu = Gpu::new();
    let initialization_ms = start.elapsed().as_secs_f64() * 1000.0;
    let names = [
        "cpu_direct_string_join",
        "cpu_packed_join",
        "gpu_packed_join",
    ];
    let mut first = Vec::new();
    // First-use processing is recorded before any warmups; GPU initialization separate.
    for (mode, name) in names.iter().enumerate() {
        let t = Instant::now();
        let output = black_box(execute(mode, &gpu, black_box(&a), black_box(&b)));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(output, want);
        first.push(ms);
        println!("First use {name}: {ms:.3} ms");
    }
    validate(&gpu);
    for _ in 0..2 {
        for mode in 0..3 {
            assert_eq!(execute(mode, &gpu, &a, &b), want);
        }
    }
    let mut timings: [Vec<f64>; 3] = Default::default();
    let mut order = Vec::new();
    // All six execution orders repeat equally to reduce cache/order bias.
    let permutations = [
        [0, 1, 2],
        [2, 1, 0],
        [1, 2, 0],
        [0, 2, 1],
        [2, 0, 1],
        [1, 0, 2],
    ];
    for trial in 0..24 {
        let modes = permutations[trial % 6];
        order.push(modes);
        for mode in modes {
            let t = Instant::now();
            let output = black_box(execute(mode, &gpu, black_box(&a), black_box(&b)));
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(output, want, "trial {trial}, mode {mode}");
            timings[mode].push(ms);
        }
        println!(
            "Trial {}/24: CPU direct {:.2} ms; CPU packed {:.2} ms; GPU full path {:.2} ms; exact output parity passed",
            trial + 1,
            timings[0][trial],
            timings[1][trial],
            timings[2][trial]
        );
    }
    let results: Vec<_>=names.iter().enumerate().map(|(i,name)|json!({"implementation":name,"first_use_ms":first[i],"warm":stats(&timings[i])})).collect();
    let matched = want.0.iter().filter(|(_, code)| *code == 1).count();
    let report = json!({"schema":"cpu-gpu-equal-boundary-v1","payments_per_source":n,"input_records":2*n,"unique_ids":want.0.len(),"matched":matched,"discrepancies":want.0.len()-matched,
        "adapter":gpu.info.name,"backend":format!("{:?}",gpu.info.backend),"driver":gpu.info.driver,"gpu_initialization_ms":initialization_ms,
        "boundary":"Borrowed typed string-ID snapshots to owned (String,u32) result rows and duplicate counters. Includes preparation, all per-batch scratch allocation/destruction, duplicate checks, comparisons, result ID ownership, and GPU upload/readback. Reused GPU context, fresh GPU buffers and fresh ID mapping every batch. Output destruction, fixture creation and parity checking excluded equally. No JSON/HTTP/persistence/full evidence reports on either path.",
        "cpu_threads":1,"warmups_per_path":2,"trials_per_path":24,"order":order,"results":results,"every_output_verified":true,
        "input_sha256":input_hash(&a,&b),"result_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&want).unwrap())),
        "source_sha256":format!("{:x}",Sha256::digest(include_bytes!("comparison.rs"))),"shader_sha256":format!("{:x}",Sha256::digest(include_bytes!("reconcile.wgsl")))});
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap())
        .expect("write comparison report");
    println!("Saved {path}; all 72 measured outputs verified");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cpu_paths_match_independent_oracle() {
        let (a, b) = fixture(1000);
        let want = oracle(&a, &b);
        assert_eq!(cpu_direct(&a, &b), want);
        let p = prepare(&a, &b);
        let (codes, errors) = cpu_packed(&p);
        let rows = p
            .ids
            .iter()
            .zip(codes)
            .map(|(id, c)| ((*id).to_owned(), c))
            .collect();
        assert_eq!((rows, errors), want);
    }
    #[test]
    fn both_cpu_paths_reject_duplicate_sources() {
        let (mut a, mut b) = fixture(100);
        a.push(a[0].clone());
        b.push(b[0].clone());
        assert_eq!(cpu_direct(&a, &b), (vec![], [1, 1, 0]));
        assert_eq!(cpu_packed(&prepare(&a, &b)), (vec![], [1, 1, 0]));
    }
}
