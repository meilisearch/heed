use std::env;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(master3)");
    // if let Some(channel) = version_check::Channel::read() {
    //     if channel.supports_features() {
    //         println!("cargo:rustc-cfg=has_specialisation");
    //     }
    // }
    let pkgname = env::var("CARGO_PKG_NAME").expect("Cargo didn't set the CARGO_PKG_NAME env var!");
    match pkgname.as_str() {
        "heed3" => println!("cargo:rustc-cfg=master3"),
        "heed" => (),
        _ => panic!("unexpected package name!"),
    }
}
