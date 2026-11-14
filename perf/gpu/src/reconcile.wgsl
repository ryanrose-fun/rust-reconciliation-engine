// Dense IDs are assigned on the host using exact string equality, not truncated hashes.
struct Payment { id: u32, low: u32, high: u32, currency: u32, status: u32 }
@group(0) @binding(0) var<storage, read> internal: array<Payment>;
@group(0) @binding(1) var<storage, read> provider: array<Payment>;
@group(0) @binding(2) var<storage, read_write> ii: array<atomic<u32>>;
@group(0) @binding(3) var<storage, read_write> pi: array<atomic<u32>>;
@group(0) @binding(4) var<storage, read_write> results: array<u32>;
@group(0) @binding(5) var<storage, read_write> errors: array<atomic<u32>>;

@compute @workgroup_size(256)
fn scatter(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if i < arrayLength(&internal) {
        let id = internal[i].id;
        if id >= arrayLength(&ii) { atomicAdd(&errors[2], 1u); }
        else if atomicExchange(&ii[id], i + 1u) != 0u { atomicAdd(&errors[0], 1u); }
    }
    if i < arrayLength(&provider) {
        let id = provider[i].id;
        if id >= arrayLength(&pi) { atomicAdd(&errors[2], 1u); }
        else if atomicExchange(&pi[id], i + 1u) != 0u { atomicAdd(&errors[1], 1u); }
    }
}

// Bits: 1 matched/present in both; 2 amount; 4 currency; 8 status;
// 16 missing provider; 32 missing internal. Zero means absent in both.
@compute @workgroup_size(256)
fn compare(@builtin(global_invocation_id) gid: vec3<u32>) {
    let id = gid.x;
    if id >= arrayLength(&results) { return; }
    let a = atomicLoad(&ii[id]);
    let b = atomicLoad(&pi[id]);
    var code = 0u;
    if a == 0u && b != 0u { code = 32u; }
    if a != 0u && b == 0u { code = 16u; }
    if a != 0u && b != 0u {
        let x = internal[a - 1u];
        let y = provider[b - 1u];
        code = 1u;
        if x.low != y.low || x.high != y.high { code |= 2u; }
        if x.currency != y.currency { code |= 4u; }
        if x.status != y.status { code |= 8u; }
    }
    results[id] = code;
}
