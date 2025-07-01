//! Expression evaluation framework for query execution.
//!
//! This module provides:
//! - Expression AST representation
//! - Type checking and validation
//! - Expression evaluation against tuples
//! - Support for various operators and functions

pub mod error;
pub mod eval;
pub mod expr;
pub mod operator;
pub mod type_checker;

pub use error::{ExpressionError, ExpressionResult};
pub use eval::{evaluate_expression, expression_to_predicate, ExpressionEvaluator, Predicate};
pub use expr::{ColumnRef, Expression, Literal};
pub use operator::{BinaryOperator, UnaryOperator};
pub use type_checker::{type_check_expression, validate_filter_predicate, TypeChecker};
