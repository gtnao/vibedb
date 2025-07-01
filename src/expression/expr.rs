//! Expression AST definitions.

use crate::access::{DataType, Value};
use crate::expression::operator::{BinaryOperator, UnaryOperator};

/// Column reference in an expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Column index in the tuple (0-based)
    pub index: usize,
    /// Optional column name for debugging/display
    pub name: Option<String>,
}

impl ColumnRef {
    pub fn new(index: usize) -> Self {
        Self { index, name: None }
    }

    pub fn with_name(index: usize, name: impl Into<String>) -> Self {
        Self {
            index,
            name: Some(name.into()),
        }
    }
}

/// Literal value in an expression
#[derive(Debug, Clone, PartialEq)]
pub struct Literal {
    pub value: Value,
}

impl Literal {
    pub fn new(value: Value) -> Self {
        Self { value }
    }

    pub fn null() -> Self {
        Self { value: Value::Null }
    }

    pub fn bool(val: bool) -> Self {
        Self {
            value: Value::Boolean(val),
        }
    }

    pub fn int32(val: i32) -> Self {
        Self {
            value: Value::Int32(val),
        }
    }

    pub fn string(val: impl Into<String>) -> Self {
        Self {
            value: Value::String(val.into()),
        }
    }
}

/// Expression tree node
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Literal constant value
    Literal(Literal),

    /// Column reference
    ColumnRef(ColumnRef),

    /// Binary operation
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },

    /// Function call (for future extensibility)
    FunctionCall { name: String, args: Vec<Expression> },

    /// CASE expression (for future extensibility)
    Case {
        conditions: Vec<(Expression, Expression)>,
        else_result: Option<Box<Expression>>,
    },

    /// IN expression (for future extensibility)
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },

    /// BETWEEN expression (for future extensibility)
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    },
}

impl Expression {
    /// Create a literal expression
    pub fn literal(value: Value) -> Self {
        Expression::Literal(Literal::new(value))
    }

    /// Create a column reference expression
    pub fn column(index: usize) -> Self {
        Expression::ColumnRef(ColumnRef::new(index))
    }

    /// Create a column reference with name
    pub fn column_with_name(index: usize, name: impl Into<String>) -> Self {
        Expression::ColumnRef(ColumnRef::with_name(index, name))
    }

    /// Create a binary operation expression
    pub fn binary_op(op: BinaryOperator, left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a unary operation expression
    pub fn unary_op(op: UnaryOperator, operand: Expression) -> Self {
        Expression::UnaryOp {
            op,
            operand: Box::new(operand),
        }
    }

    /// Create an AND expression
    pub fn and(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::And, left, right)
    }

    /// Create an OR expression
    pub fn or(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Or, left, right)
    }

    /// Create a NOT expression
    pub fn not_expr(operand: Expression) -> Self {
        Self::unary_op(UnaryOperator::Not, operand)
    }

    /// Create an equality expression
    pub fn eq(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Eq, left, right)
    }

    /// Create a not-equal expression
    pub fn ne(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Ne, left, right)
    }

    /// Create a less-than expression
    pub fn lt(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Lt, left, right)
    }

    /// Create a less-than-or-equal expression
    pub fn le(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Le, left, right)
    }

    /// Create a greater-than expression
    pub fn gt(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Gt, left, right)
    }

    /// Create a greater-than-or-equal expression
    pub fn ge(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Ge, left, right)
    }

    /// Create an addition expression
    pub fn add_expr(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Add, left, right)
    }

    /// Create a subtraction expression
    pub fn sub_expr(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Sub, left, right)
    }

    /// Create a multiplication expression
    pub fn mul_expr(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Mul, left, right)
    }

    /// Create a division expression
    pub fn div_expr(left: Expression, right: Expression) -> Self {
        Self::binary_op(BinaryOperator::Div, left, right)
    }

    /// Create an IS NULL expression
    pub fn is_null(operand: Expression) -> Self {
        Self::unary_op(UnaryOperator::IsNull, operand)
    }

    /// Create an IS NOT NULL expression
    pub fn is_not_null(operand: Expression) -> Self {
        Self::unary_op(UnaryOperator::IsNotNull, operand)
    }

    /// Check if this expression is a constant (contains no column references)
    pub fn is_constant(&self) -> bool {
        match self {
            Expression::Literal(_) => true,
            Expression::ColumnRef(_) => false,
            Expression::BinaryOp { left, right, .. } => left.is_constant() && right.is_constant(),
            Expression::UnaryOp { operand, .. } => operand.is_constant(),
            Expression::FunctionCall { args, .. } => args.iter().all(|arg| arg.is_constant()),
            Expression::Case {
                conditions,
                else_result,
            } => {
                conditions
                    .iter()
                    .all(|(cond, res)| cond.is_constant() && res.is_constant())
                    && else_result
                        .as_ref()
                        .map(|e| e.is_constant())
                        .unwrap_or(true)
            }
            Expression::In { expr, list, .. } => {
                expr.is_constant() && list.iter().all(|e| e.is_constant())
            }
            Expression::Between {
                expr, low, high, ..
            } => expr.is_constant() && low.is_constant() && high.is_constant(),
        }
    }

    /// Get the expected output type of this expression (if it can be determined statically)
    pub fn output_type(&self, input_schema: &[DataType]) -> Option<DataType> {
        match self {
            Expression::Literal(lit) => lit.value.data_type(),
            Expression::ColumnRef(col) => input_schema.get(col.index).copied(),
            Expression::BinaryOp { op, left, right } => {
                let left_type = left.output_type(input_schema)?;
                let right_type = right.output_type(input_schema)?;
                op.output_type(left_type, right_type)
            }
            Expression::UnaryOp { op, operand } => {
                let operand_type = operand.output_type(input_schema)?;
                op.output_type(operand_type)
            }
            // For now, function calls, case, in, and between return None
            // This can be extended in the future
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref() {
        let col1 = ColumnRef::new(0);
        assert_eq!(col1.index, 0);
        assert!(col1.name.is_none());

        let col2 = ColumnRef::with_name(1, "age");
        assert_eq!(col2.index, 1);
        assert_eq!(col2.name.as_deref(), Some("age"));
    }

    #[test]
    fn test_literal() {
        let null_lit = Literal::null();
        assert_eq!(null_lit.value, Value::Null);

        let bool_lit = Literal::bool(true);
        assert_eq!(bool_lit.value, Value::Boolean(true));

        let int_lit = Literal::int32(42);
        assert_eq!(int_lit.value, Value::Int32(42));

        let str_lit = Literal::string("hello");
        assert_eq!(str_lit.value, Value::String("hello".to_string()));
    }

    #[test]
    fn test_expression_builders() {
        // Test literal
        let expr1 = Expression::literal(Value::Int32(10));
        matches!(expr1, Expression::Literal(_));

        // Test column reference
        let expr2 = Expression::column(0);
        matches!(expr2, Expression::ColumnRef(_));

        // Test binary operations
        let expr3 =
            Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(5)));
        matches!(expr3, Expression::BinaryOp { .. });

        // Test comparison
        let expr4 = Expression::gt(Expression::column(1), Expression::literal(Value::Int32(18)));
        matches!(expr4, Expression::BinaryOp { .. });

        // Test logical operations
        let expr5 = Expression::and(
            Expression::eq(Expression::column(0), Expression::literal(Value::Int32(1))),
            Expression::is_not_null(Expression::column(1)),
        );
        matches!(expr5, Expression::BinaryOp { .. });
    }

    #[test]
    fn test_is_constant() {
        // Literal is constant
        assert!(Expression::literal(Value::Int32(42)).is_constant());

        // Column reference is not constant
        assert!(!Expression::column(0).is_constant());

        // Binary op with all constants is constant
        assert!(Expression::add_expr(
            Expression::literal(Value::Int32(1)),
            Expression::literal(Value::Int32(2))
        )
        .is_constant());

        // Binary op with column reference is not constant
        assert!(
            !Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(2)))
                .is_constant()
        );

        // Unary op with constant is constant
        assert!(Expression::not_expr(Expression::literal(Value::Boolean(true))).is_constant());

        // Unary op with column is not constant
        assert!(!Expression::is_null(Expression::column(0)).is_constant());
    }

    #[test]
    fn test_output_type() {
        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Boolean];

        // Literal types
        assert_eq!(
            Expression::literal(Value::Int32(42)).output_type(&schema),
            Some(DataType::Int32)
        );
        assert_eq!(
            Expression::literal(Value::String("test".to_string())).output_type(&schema),
            Some(DataType::Varchar)
        );
        assert_eq!(
            Expression::literal(Value::Boolean(true)).output_type(&schema),
            Some(DataType::Boolean)
        );
        assert_eq!(Expression::literal(Value::Null).output_type(&schema), None);

        // Column reference types
        assert_eq!(
            Expression::column(0).output_type(&schema),
            Some(DataType::Int32)
        );
        assert_eq!(
            Expression::column(1).output_type(&schema),
            Some(DataType::Varchar)
        );
        assert_eq!(
            Expression::column(2).output_type(&schema),
            Some(DataType::Boolean)
        );
        assert_eq!(Expression::column(3).output_type(&schema), None); // Out of bounds
    }
}
