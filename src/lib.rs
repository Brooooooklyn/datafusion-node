#![deny(clippy::all)]
#![allow(clippy::new_without_default)]

#[cfg(all(
  not(all(target_os = "linux", target_env = "musl", target_arch = "aarch64")),
  not(debug_assertions)
))]
#[global_allocator]
static ALLOC: mimalloc_rust::GlobalMiMalloc = mimalloc_rust::GlobalMiMalloc;

pub mod data_frame;
pub mod expr;
pub mod session_context;
