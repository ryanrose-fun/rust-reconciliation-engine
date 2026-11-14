struct Value { vlo: u32, vhi: u32, low: u32, high: u32, currency: u32, status: u32 }
struct Patch { id: u32, mask: u32, internal: Value, provider: Value }
@group(0) @binding(0) var<storage, read_write> internal: array<Value>;
@group(0) @binding(1) var<storage, read_write> provider: array<Value>;
@group(0) @binding(2) var<storage, read_write> results: array<u32>;
@group(0) @binding(3) var<storage, read> patches: array<Patch>;
@group(0) @binding(4) var<storage, read_write> receipts: array<vec4<u32>>;
@group(0) @binding(5) var<storage, read_write> changes: array<vec4<u32>>;
@group(0) @binding(6) var<storage, read_write> changed: atomic<u32>;
@group(0) @binding(7) var<uniform> params: vec4<u32>;

// 0 not submitted; 1 applied; 2 exact retry; 3 same-version conflict; 4 stale.
fn outcome(old: Value, next: Value) -> u32 {
    if next.vhi < old.vhi || (next.vhi == old.vhi && next.vlo < old.vlo) { return 4u; }
    if next.vlo == old.vlo && next.vhi == old.vhi {
        if next.low == old.low && next.high == old.high && next.currency == old.currency && next.status == old.status { return 2u; }
        return 3u;
    }
    return 1u;
}
fn compare(a: Value, b: Value) -> u32 {
    let has_a = a.vlo != 0u || a.vhi != 0u;
    let has_b = b.vlo != 0u || b.vhi != 0u;
    if !has_a { return select(0u,32u,has_b); }
    if !has_b { return 16u; }
    var code = 1u;
    if a.low != b.low || a.high != b.high { code |= 2u; }
    if a.currency != b.currency { code |= 4u; }
    if a.status != b.status { code |= 8u; }
    return code;
}
@compute @workgroup_size(256)
fn update(@builtin(global_invocation_id) gid: vec3<u32>) {
    if gid.x >= params.x { return; }
    let p = patches[gid.x];
    // Host guarantees one patch per ID in a batch; no cross-invocation state race.
    var a = internal[p.id]; var b = provider[p.id];
    var ai = 0u; var bi = 0u;
    if (p.mask & 1u) != 0u {
        ai = outcome(a,p.internal);
        if ai == 1u { a = p.internal; internal[p.id] = a; }
    }
    if (p.mask & 2u) != 0u {
        bi = outcome(b,p.provider);
        if bi == 1u { b = p.provider; provider[p.id] = b; }
    }
    let before = results[p.id]; let after = compare(a,b);
    results[p.id] = after;
    receipts[gid.x] = vec4<u32>(p.id,ai,bi,after);
    if before != after {
        let slot = atomicAdd(&changed,1u);
        changes[slot] = vec4<u32>(p.id,before,after,0u);
    }
}
