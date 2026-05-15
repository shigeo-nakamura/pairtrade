use std::process::Command;

fn git_hash(dir: &str) -> String {
    Command::new("git")
        .args(["-C", dir, "rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() {
    println!("cargo:rerun-if-changed=../dex-connector/.git/HEAD");
    println!("cargo:rerun-if-changed=../dex-connector/.git/refs/heads");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");

    println!(
        "cargo:rustc-env=DEX_CONNECTOR_GIT_HASH={}",
        git_hash("../dex-connector")
    );
    println!("cargo:rustc-env=PAIRTRADE_GIT_SHA={}", git_hash("."));
}
