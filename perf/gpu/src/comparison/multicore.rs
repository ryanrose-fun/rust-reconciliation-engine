use super::*;
use std::{collections::hash_map::RandomState, hash::BuildHasher, thread};

// Hashes choose a worker only. Full string equality still decides payment identity.
// Each worker owns its index: no shared mutable map or per-payment locking.
fn reconcile(a: &[Input], b: &[Input], workers: usize) -> Output {
    assert!(workers > 0);
    let count = a.len() + b.len();
    if count == 0 {
        return (vec![], [0; 3]);
    }
    let routing = RandomState::new();
    let chunk = count.div_ceil(workers);
    let routed = thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let routing = &routing;
                scope.spawn(move || {
                    let mut buckets: Vec<Vec<(usize, &Input)>> =
                        (0..workers).map(|_| Vec::new()).collect();
                    for position in (worker * chunk)..((worker + 1) * chunk).min(count) {
                        let p = if position < a.len() {
                            &a[position]
                        } else {
                            &b[position - a.len()]
                        };
                        let shard = (routing.hash_one(&p.id) % workers as u64) as usize;
                        buckets[shard].push((position, p));
                    }
                    buckets
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("routing worker panicked"))
            .collect::<Vec<_>>()
    });
    let mut shards: Vec<Vec<(usize, &Input)>> = (0..workers).map(|_| Vec::new()).collect();
    for buckets in routed {
        for (i, mut bucket) in buckets.into_iter().enumerate() {
            shards[i].append(&mut bucket);
        }
    }
    let joined = thread::scope(|scope| {
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                scope.spawn(move || {
                    let mut index: HashMap<&str, usize> = HashMap::with_capacity(shard.len() / 2);
                    let mut pairs: Vec<(usize, Option<&Input>, Option<&Input>)> =
                        Vec::with_capacity(shard.len() / 2);
                    let mut errors = [0; 3];
                    for (position, p) in shard {
                        let i = *index.entry(p.id.as_str()).or_insert_with(|| {
                            let i = pairs.len();
                            pairs.push((position, None, None));
                            i
                        });
                        let pair = &mut pairs[i];
                        pair.0 = pair.0.min(position);
                        let source = usize::from(position >= a.len());
                        let slot = if source == 0 {
                            &mut pair.1
                        } else {
                            &mut pair.2
                        };
                        if slot.replace(p).is_some() {
                            errors[source] += 1;
                        }
                    }
                    let rows = if errors == [0; 3] {
                        pairs
                            .into_iter()
                            .map(|(position, x, y)| {
                                let id = &x.or(y).unwrap().id;
                                (position, (id.clone(), classify(x, y)))
                            })
                            .collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    (rows, errors)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("join worker panicked"))
            .collect::<Vec<_>>()
    });
    let mut errors = [0; 3];
    for (_, e) in &joined {
        for i in 0..3 {
            errors[i] += e[i];
        }
    }
    if errors != [0; 3] {
        return (vec![], errors);
    }
    // Restore first-internal-then-provider-only order without a comparison sort.
    let mut ordered: Vec<Option<(String, u32)>> =
        std::iter::repeat_with(|| None).take(count).collect();
    for (rows, _) in joined {
        for (position, row) in rows {
            ordered[position] = Some(row);
        }
    }
    (ordered.into_iter().flatten().collect(), errors)
}

pub(crate) fn run(n: usize, path: &str) {
    let (a, b) = fixture(n);
    let want = oracle(&a, &b);
    let start = Instant::now();
    let gpu = Gpu::new();
    let init_ms = start.elapsed().as_secs_f64() * 1000.0;
    let workers = [1, 4, 8, 16];
    let names = [
        "cpu_direct_string_join",
        "cpu_packed_join",
        "gpu_packed_join",
        "cpu_sharded_1",
        "cpu_sharded_4",
        "cpu_sharded_8",
        "cpu_sharded_16",
    ];
    let call = |mode: usize| {
        if mode < 3 {
            execute(mode, &gpu, &a, &b)
        } else {
            reconcile(&a, &b, workers[mode - 3])
        }
    };
    let mut first = vec![];
    for (mode, name) in names.iter().enumerate() {
        let t = Instant::now();
        let out = black_box(call(mode));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(out, want);
        first.push(ms);
        println!("First use {name}: {ms:.2} ms; verified");
    }
    for _ in 0..2 {
        for mode in 0..names.len() {
            assert_eq!(call(mode), want);
        }
    }
    let trials = names.len() * 2;
    let mut timings = vec![Vec::new(); names.len()];
    let mut orders = vec![];
    for trial in 0..trials {
        let mut order: Vec<_> = (0..names.len())
            .map(|i| (i + trial) % names.len())
            .collect();
        if trial >= names.len() {
            order.reverse();
        }
        for &mode in &order {
            let start = Instant::now();
            let out = black_box(call(mode));
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(out, want, "mode {mode}, trial {trial}");
            timings[mode].push(ms);
        }
        orders.push(order);
        println!(
            "Trial {}/{}: direct {:.1}; packed {:.1}; GPU {:.1}; sharded 1/4/8/16: {:.1}/{:.1}/{:.1}/{:.1} ms; verified",
            trial + 1,
            trials,
            timings[0][trial],
            timings[1][trial],
            timings[2][trial],
            timings[3][trial],
            timings[4][trial],
            timings[5][trial],
            timings[6][trial]
        );
    }
    let results:Vec<_>=names.iter().enumerate().map(|(i,name)|json!({"implementation":name,"first_use_ms":first[i],"warm":stats(&timings[i])})).collect();
    let report = json!({"schema":"multicore-equal-boundary-v1","payments_per_source":n,"input_records":2*n,"unique_ids":want.0.len(),"results":results,"trials_per_path":trials,"warmups_per_path":2,"order":orders,"every_output_verified":true,
        "boundary":"Same typed string-ID input to identical owned ID/result rows and duplicate counters. Fresh preparation, allocation, scratch destruction, output creation and GPU transfers included. Multicore creates/joins scoped threads inside each timed call; no pool initialization excluded. GPU device reused; fresh buffers every batch. Fixture creation, output destruction and parity verification excluded equally. No HTTP, JSON, database or full evidence report.",
        "adapter":gpu.info.name,"gpu_initialization_ms":init_ms,"logical_processors":thread::available_parallelism().unwrap().get(),"workers":workers,
        "input_sha256":input_hash(&a,&b),"result_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&want).unwrap())),"source_sha256":format!("{:x}",Sha256::digest(include_bytes!("multicore.rs")))});
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!("Saved {path}");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shuffled_results_and_order_match_oracle_for_worker_counts() {
        for n in [1, 17, 1001] {
            let (a, b) = fixture(n);
            let want = oracle(&a, &b);
            for w in [1, 2, 4, 8, 16] {
                assert_eq!(reconcile(&a, &b, w), want);
            }
        }
    }
    #[test]
    fn duplicates_across_chunks_are_rejected_in_both_sources() {
        let (mut a, mut b) = fixture(103);
        a.push(a[0].clone());
        a.push(a[0].clone());
        b.push(b[0].clone());
        for w in [1, 4, 16] {
            assert_eq!(reconcile(&a, &b, w), (vec![], [2, 1, 0]));
        }
    }
    #[test]
    fn empty_and_one_sided_inputs_preserve_order() {
        let (a, b) = fixture(31);
        for (x, y) in [(&a[..], &[][..]), (&[][..], &b[..]), (&[][..], &[][..])] {
            for w in [1, 4] {
                assert_eq!(reconcile(x, y, w), oracle(x, y));
            }
        }
    }
    #[test]
    fn exact_ids_and_u64_values_survive_shared_shard() {
        let a = vec![
            Input {
                id: "a".into(),
                amount: u64::MAX,
                currency: 0,
                status: 1,
            },
            Input {
                id: "A".into(),
                amount: 0,
                currency: 3,
                status: 2,
            },
        ];
        let mut b = a.clone();
        b[0].amount -= 1u64 << 32;
        b[1].currency = 0;
        b[1].status = 0;
        assert_eq!(reconcile(&a, &b, 1), oracle(&a, &b));
    }
}
