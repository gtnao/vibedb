# vibedb Development Rules

## Coding Rules

1. **Module Management**
   - Do not use `mod.rs`
   - Define modules directly in parent files

2. **Testing**
   - Write tests in the same file as implementation (except E2E tests)
   - Write tests inside `#[cfg(test)]` module

3. **Quality Assurance**
   - Ensure all following commands pass:
     - `cargo build`
     - `cargo test`
     - `cargo fmt`
     - `cargo clippy`

4. **Version Control**
   - Execute `git commit` and `git push` after all commands pass