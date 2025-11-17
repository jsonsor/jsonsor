use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod stream;
mod chunk;
pub mod field_func;
pub mod jsonsor;
