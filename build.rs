//! Build script: on macOS, ScreenCaptureKit's Swift bridge (via the
//! `screencapturekit` / `apple-cf` crates) links `libswift_Concurrency.dylib`
//! through `@rpath`, but the final binary ships no `LC_RPATH`, so it fails to
//! launch with "Library not loaded: @rpath/libswift_Concurrency.dylib".
//!
//! Add the OS Swift runtime directory (resolved via the dyld shared cache) plus
//! the Command Line Tools fallback so the loader can find it.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/lib/swift");
        println!(
            "cargo:rustc-link-arg=-Wl,-rpath,/Library/Developer/CommandLineTools/usr/lib/swift-5.5/macosx"
        );
    }
}
