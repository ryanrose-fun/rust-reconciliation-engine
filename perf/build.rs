fn main() {
    println!("cargo:rerun-if-changed=../Cargo.toml");
    let text = std::fs::read_to_string("../Cargo.toml").unwrap();
    let version = text
        .lines()
        .find_map(|l| {
            l.strip_prefix("version = \"")
                .map(|s| s.trim_end_matches('"'))
        })
        .unwrap();
    println!("cargo:rustc-env=ENGINE_VERSION={version}");
}
