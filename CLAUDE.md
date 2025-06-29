# vibedb Development Rules

## Coding Rules

1. **Module Management - CRITICAL RULE**
   - **NEVER CREATE mod.rs FILES** - This is the #1 most important rule
   - Define modules directly in parent files
   - Example of CORRECT module structure:
     ```
     src/
       lib.rs          # Contains: pub mod catalog;
       catalog.rs      # Main catalog module
       catalog/        # Submodules directory
         table_info.rs
         column_info.rs
     ```
   - In catalog.rs, declare submodules like:
     ```rust
     pub mod table_info;
     pub mod column_info;
     ```
   - Run `./check_no_mod_rs.sh` before any commit to verify no mod.rs exists

2. **Testing**
   - Write tests in the same file as implementation (except E2E tests)
   - Write tests inside `#[cfg(test)]` module

3. **Quality Assurance**
   - Ensure all following commands pass:
     - `./check_no_mod_rs.sh` (MUST run this first!)
     - `cargo build`
     - `cargo test`
     - `cargo fmt`
     - `cargo clippy`

4. **Version Control**
   - Execute `git commit` and `git push` after all commands pass