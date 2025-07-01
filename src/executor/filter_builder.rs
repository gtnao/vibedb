//! Builder pattern for creating filter expressions easily.
//!
//! This module provides a fluent API for building filter expressions
//! that can be used with FilterExecutor.

use crate::access::Value;
use crate::expression::Expression;

/// Builder for creating filter expressions
pub struct FilterBuilder;

impl FilterBuilder {
    /// Create a new filter builder
    pub fn new() -> Self {
        Self
    }

    /// Create a column reference expression
    pub fn column(index: usize) -> Expression {
        Expression::column(index)
    }

    /// Create a column reference with name
    pub fn column_with_name(index: usize, name: impl Into<String>) -> Expression {
        Expression::column_with_name(index, name)
    }

    /// Create a literal value expression
    pub fn value(val: Value) -> Expression {
        Expression::literal(val)
    }

    /// Create an integer literal
    pub fn int32(val: i32) -> Expression {
        Expression::literal(Value::Int32(val))
    }

    /// Create a string literal
    pub fn string(val: impl Into<String>) -> Expression {
        Expression::literal(Value::String(val.into()))
    }

    /// Create a boolean literal
    pub fn boolean(val: bool) -> Expression {
        Expression::literal(Value::Boolean(val))
    }

    /// Create a NULL literal
    pub fn null() -> Expression {
        Expression::literal(Value::Null)
    }

    /// Create an equality comparison (column = value)
    pub fn eq(left: Expression, right: Expression) -> Expression {
        Expression::eq(left, right)
    }

    /// Create a not-equal comparison (column != value)
    pub fn ne(left: Expression, right: Expression) -> Expression {
        Expression::ne(left, right)
    }

    /// Create a less-than comparison (column < value)
    pub fn lt(left: Expression, right: Expression) -> Expression {
        Expression::lt(left, right)
    }

    /// Create a less-than-or-equal comparison (column <= value)
    pub fn le(left: Expression, right: Expression) -> Expression {
        Expression::le(left, right)
    }

    /// Create a greater-than comparison (column > value)
    pub fn gt(left: Expression, right: Expression) -> Expression {
        Expression::gt(left, right)
    }

    /// Create a greater-than-or-equal comparison (column >= value)
    pub fn ge(left: Expression, right: Expression) -> Expression {
        Expression::ge(left, right)
    }

    /// Create an AND expression
    pub fn and(left: Expression, right: Expression) -> Expression {
        Expression::and(left, right)
    }

    /// Create an OR expression
    pub fn or(left: Expression, right: Expression) -> Expression {
        Expression::or(left, right)
    }

    /// Create a NOT expression
    pub fn not_expr(expr: Expression) -> Expression {
        Expression::not_expr(expr)
    }

    /// Create an IS NULL expression
    pub fn is_null(expr: Expression) -> Expression {
        Expression::is_null(expr)
    }

    /// Create an IS NOT NULL expression
    pub fn is_not_null(expr: Expression) -> Expression {
        Expression::is_not_null(expr)
    }

    /// Create a string LIKE expression (simple pattern matching)
    /// Note: This is a simplified version that checks if a string contains a pattern
    pub fn like(column_expr: Expression, pattern: impl Into<String>) -> Expression {
        // For now, we'll use a function call expression
        // In the future, this could be optimized with a specific LIKE operator
        Expression::FunctionCall {
            name: "like".to_string(),
            args: vec![
                column_expr,
                Expression::literal(Value::String(pattern.into())),
            ],
        }
    }

    /// Create a string ends_with expression
    pub fn ends_with(column_expr: Expression, suffix: impl Into<String>) -> Expression {
        Expression::FunctionCall {
            name: "ends_with".to_string(),
            args: vec![
                column_expr,
                Expression::literal(Value::String(suffix.into())),
            ],
        }
    }

    /// Create a string starts_with expression
    pub fn starts_with(column_expr: Expression, prefix: impl Into<String>) -> Expression {
        Expression::FunctionCall {
            name: "starts_with".to_string(),
            args: vec![
                column_expr,
                Expression::literal(Value::String(prefix.into())),
            ],
        }
    }
}

/// Convenience functions for common filter patterns
impl FilterBuilder {
    /// Create a filter for column equals value
    pub fn column_equals(column_idx: usize, value: Value) -> Expression {
        Self::eq(Self::column(column_idx), Self::value(value))
    }

    /// Create a filter for column equals string
    pub fn column_equals_string(column_idx: usize, value: impl Into<String>) -> Expression {
        Self::eq(Self::column(column_idx), Self::string(value))
    }

    /// Create a filter for column equals int32
    pub fn column_equals_int32(column_idx: usize, value: i32) -> Expression {
        Self::eq(Self::column(column_idx), Self::int32(value))
    }

    /// Create a filter for column greater than value
    pub fn column_gt(column_idx: usize, value: Value) -> Expression {
        Self::gt(Self::column(column_idx), Self::value(value))
    }

    /// Create a filter for column greater than int32
    pub fn column_gt_int32(column_idx: usize, value: i32) -> Expression {
        Self::gt(Self::column(column_idx), Self::int32(value))
    }

    /// Create a filter for column greater than or equal to value
    pub fn column_ge(column_idx: usize, value: Value) -> Expression {
        Self::ge(Self::column(column_idx), Self::value(value))
    }

    /// Create a filter for column greater than or equal to int32
    pub fn column_ge_int32(column_idx: usize, value: i32) -> Expression {
        Self::ge(Self::column(column_idx), Self::int32(value))
    }

    /// Create a filter for column less than value
    pub fn column_lt(column_idx: usize, value: Value) -> Expression {
        Self::lt(Self::column(column_idx), Self::value(value))
    }

    /// Create a filter for column less than int32
    pub fn column_lt_int32(column_idx: usize, value: i32) -> Expression {
        Self::lt(Self::column(column_idx), Self::int32(value))
    }

    /// Create a filter for column less than or equal to value
    pub fn column_le(column_idx: usize, value: Value) -> Expression {
        Self::le(Self::column(column_idx), Self::value(value))
    }

    /// Create a filter for column less than or equal to int32
    pub fn column_le_int32(column_idx: usize, value: i32) -> Expression {
        Self::le(Self::column(column_idx), Self::int32(value))
    }

    /// Create a filter for column is null
    pub fn column_is_null(column_idx: usize) -> Expression {
        Self::is_null(Self::column(column_idx))
    }

    /// Create a filter for column is not null
    pub fn column_is_not_null(column_idx: usize) -> Expression {
        Self::is_not_null(Self::column(column_idx))
    }
}

impl Default for FilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_filters() {
        // Test column equals
        let expr = FilterBuilder::column_equals_int32(0, 42);
        matches!(expr, Expression::BinaryOp { .. });

        // Test column greater than
        let expr = FilterBuilder::column_gt_int32(1, 100);
        matches!(expr, Expression::BinaryOp { .. });

        // Test column is null
        let expr = FilterBuilder::column_is_null(2);
        matches!(expr, Expression::UnaryOp { .. });
    }

    #[test]
    fn test_complex_filters() {
        // Test AND condition
        let expr = FilterBuilder::and(
            FilterBuilder::column_equals_string(1, "Engineering"),
            FilterBuilder::column_gt_int32(2, 50000),
        );
        matches!(expr, Expression::BinaryOp { .. });

        // Test OR condition
        let expr = FilterBuilder::or(
            FilterBuilder::column_equals_string(1, "Sales"),
            FilterBuilder::column_equals_string(1, "Marketing"),
        );
        matches!(expr, Expression::BinaryOp { .. });

        // Test NOT condition
        let expr = FilterBuilder::not_expr(FilterBuilder::column_equals_string(1, "HR"));
        matches!(expr, Expression::UnaryOp { .. });
    }

    #[test]
    fn test_builder_methods() {
        let _fb = FilterBuilder::new();

        // Test creating literals
        let _int = FilterBuilder::int32(42);
        let _str = FilterBuilder::string("test");
        let _bool = FilterBuilder::boolean(true);
        let _null = FilterBuilder::null();

        // Test creating column references
        let _col = FilterBuilder::column(0);
        let _named_col = FilterBuilder::column_with_name(1, "name");
    }
}
