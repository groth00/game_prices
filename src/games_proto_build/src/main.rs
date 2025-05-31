use std::error::Error;

pub mod codegen;

fn main() -> Result<(), Box<dyn Error>> {
    codegen::generate_protos()?;

    Ok(())
}
