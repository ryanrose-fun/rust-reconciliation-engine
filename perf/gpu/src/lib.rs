//! Resident GPU reconciliation compute. Durability belongs to the API journal.
use bytemuck::{Pod, Zeroable};
pub mod resident;

pub struct Gpu {
    pub(crate) device: wgpu::Device,
    pub(crate) queue: wgpu::Queue,
    pub info: wgpu::AdapterInfo,
}
impl Gpu {
    pub fn try_new() -> Result<Self, String> {
        let backend = if cfg!(target_os = "windows") {
            wgpu::Backends::DX12
        } else {
            wgpu::Backends::VULKAN
        };
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: backend,
            ..Default::default()
        });
        let selection = std::env::var("RECONCILE_GPU_ADAPTER").unwrap_or_default();
        let adapter = instance
            .enumerate_adapters(backend)
            .into_iter()
            .find(|a| {
                let info = a.get_info();
                info.device_type == wgpu::DeviceType::DiscreteGpu && info.name.contains(&selection)
            })
            .ok_or("No matching discrete GPU; CPU/software fallback is disabled")?;
        let info = adapter.get_info();
        let (device, queue) = pollster::block_on(adapter.request_device(
            &wgpu::DeviceDescriptor {
                label: Some("Rust GPU Finance"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                memory_hints: wgpu::MemoryHints::Performance,
            },
            None,
        ))
        .map_err(|e| e.to_string())?;
        Ok(Self {
            device,
            queue,
            info,
        })
    }
    pub fn new() -> Self {
        Self::try_new().expect("discrete GPU initialization")
    }
}
impl Default for Gpu {
    fn default() -> Self {
        Self::new()
    }
}
fn stats(values: &[f64]) -> serde_json::Value {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    serde_json::json!({"samples_ms":values,"min_ms":v[0],"median_ms":(v[(v.len()-1)/2]+v[v.len()/2])/2.0,"max_ms":v[v.len()-1]})
}
