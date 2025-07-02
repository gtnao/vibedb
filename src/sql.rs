// SQL module - SQL parsing and AST representation

pub mod ast;
pub mod lexer;
pub mod parser;
pub mod statement;
pub mod token;

pub use ast::*;
pub use lexer::Lexer;
pub use parser::Parser;
pub use statement::*;
pub use token::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_module_imports() {
        // Basic test to ensure module structure is correct
        let _ = Token::Identifier("test".to_string());
        assert!(true);
    }
}
