use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod chunk;
pub mod schema;
pub mod stream;
pub mod field_func;
pub mod jsonsor;
pub mod jsonsor_par;

#[cfg(feature = "arrow")]
pub mod arrow;
