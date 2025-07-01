//! Type checking for expressions.

use crate::access::DataType;
use crate::expression::{Expression, ExpressionError, ExpressionResult};

/// Type checker for expressions
pub struct TypeChecker<'a> {
    /// Schema defining the types of input columns
    schema: &'a [DataType],
}

impl<'a> TypeChecker<'a> {
    /// Create a new type checker with the given schema
    pub fn new(schema: &'a [DataType]) -> Self {
        Self { schema }
    }

    /// Type check an expression and return its output type
    pub fn check(&self, expr: &Expression) -> ExpressionResult<Option<DataType>> {
        match expr {
            Expression::Literal(lit) => Ok(lit.value.data_type()),

            Expression::ColumnRef(col) => {
                if col.index >= self.schema.len() {
                    return Err(ExpressionError::ColumnIndexOutOfBounds {
                        index: col.index,
                        tuple_size: self.schema.len(),
                    });
                }
                Ok(Some(self.schema[col.index]))
            }

            Expression::BinaryOp { op, left, right } => {
                let left_type = self.check(left)?;
                let right_type = self.check(right)?;

                // Both operands must have known types for binary operations
                match (left_type, right_type) {
                    (Some(lt), Some(rt)) => match op.output_type(lt, rt) {
                        Some(output_type) => Ok(Some(output_type)),
                        None => Err(ExpressionError::InvalidOperandTypes {
                            operator: op.as_str().to_string(),
                            left_type: Some(lt),
                            right_type: Some(rt),
                        }),
                    },
                    // NULL literals are allowed and will be handled at runtime
                    (None, Some(_)) | (Some(_), None) | (None, None) => Ok(None),
                }
            }

            Expression::UnaryOp { op, operand } => {
                let operand_type = self.check(operand)?;

                match operand_type {
                    Some(ot) => match op.output_type(ot) {
                        Some(output_type) => Ok(Some(output_type)),
                        None => Err(ExpressionError::InvalidOperandTypes {
                            operator: op.as_str().to_string(),
                            left_type: Some(ot),
                            right_type: None,
                        }),
                    },
                    // NULL literals are allowed
                    None => Ok(None),
                }
            }

            Expression::FunctionCall { name, .. } => {
                Err(ExpressionError::UnknownFunction { name: name.clone() })
            }

            Expression::Case { .. } => Err(ExpressionError::TypeCheckFailed {
                expression: "CASE".to_string(),
                reason: "CASE expressions not yet implemented".to_string(),
            }),

            Expression::In { .. } => Err(ExpressionError::TypeCheckFailed {
                expression: "IN".to_string(),
                reason: "IN expressions not yet implemented".to_string(),
            }),

            Expression::Between { .. } => Err(ExpressionError::TypeCheckFailed {
                expression: "BETWEEN".to_string(),
                reason: "BETWEEN expressions not yet implemented".to_string(),
            }),
        }
    }

    /// Check if an expression is valid for use as a filter predicate
    pub fn check_filter_predicate(&self, expr: &Expression) -> ExpressionResult<()> {
        let output_type = self.check(expr)?;

        match output_type {
            Some(DataType::Boolean) | None => Ok(()), // Boolean or NULL is OK for filters
            Some(other_type) => Err(ExpressionError::TypeMismatch {
                expected: DataType::Boolean,
                actual: other_type,
                context: "filter predicate".to_string(),
            }),
        }
    }

    /// Check if an expression is valid for use in a projection
    pub fn check_projection(&self, expr: &Expression) -> ExpressionResult<Option<DataType>> {
        // Any expression type is valid in a projection
        self.check(expr)
    }

    /// Check if two expressions have compatible types
    pub fn check_compatible(&self, expr1: &Expression, expr2: &Expression) -> ExpressionResult<()> {
        let type1 = self.check(expr1)?;
        let type2 = self.check(expr2)?;

        match (type1, type2) {
            (Some(t1), Some(t2)) if t1 == t2 => Ok(()),
            (None, _) | (_, None) => Ok(()), // NULL is compatible with any type
            (Some(t1), Some(t2)) => Err(ExpressionError::TypeMismatch {
                expected: t1,
                actual: t2,
                context: "type compatibility check".to_string(),
            }),
        }
    }
}

/// Helper function to type check an expression
pub fn type_check_expression(
    expr: &Expression,
    schema: &[DataType],
) -> ExpressionResult<Option<DataType>> {
    TypeChecker::new(schema).check(expr)
}

/// Helper function to validate a filter predicate
pub fn validate_filter_predicate(expr: &Expression, schema: &[DataType]) -> ExpressionResult<()> {
    TypeChecker::new(schema).check_filter_predicate(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::Value;
    use crate::expression::{BinaryOperator, UnaryOperator};

    #[test]
    fn test_literal_type_checking() {
        let schema = vec![];
        let checker = TypeChecker::new(&schema);

        // Various literal types
        assert_eq!(
            checker
                .check(&Expression::literal(Value::Int32(42)))
                .unwrap(),
            Some(DataType::Int32)
        );
        assert_eq!(
            checker
                .check(&Expression::literal(Value::String("test".to_string())))
                .unwrap(),
            Some(DataType::Varchar)
        );
        assert_eq!(
            checker
                .check(&Expression::literal(Value::Boolean(true)))
                .unwrap(),
            Some(DataType::Boolean)
        );
        assert_eq!(
            checker.check(&Expression::literal(Value::Null)).unwrap(),
            None
        );
    }

    #[test]
    fn test_column_ref_type_checking() {
        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Boolean];
        let checker = TypeChecker::new(&schema);

        assert_eq!(
            checker.check(&Expression::column(0)).unwrap(),
            Some(DataType::Int32)
        );
        assert_eq!(
            checker.check(&Expression::column(1)).unwrap(),
            Some(DataType::Varchar)
        );
        assert_eq!(
            checker.check(&Expression::column(2)).unwrap(),
            Some(DataType::Boolean)
        );

        // Out of bounds
        assert!(matches!(
            checker.check(&Expression::column(3)),
            Err(ExpressionError::ColumnIndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn test_binary_op_type_checking() {
        let schema = vec![DataType::Int32, DataType::Varchar, DataType::Boolean];
        let checker = TypeChecker::new(&schema);

        // Valid arithmetic
        let expr =
            Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(5)));
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Int32));

        // Invalid arithmetic (type mismatch)
        let expr = Expression::add_expr(Expression::column(0), Expression::column(1));
        assert!(matches!(
            checker.check(&expr),
            Err(ExpressionError::InvalidOperandTypes { .. })
        ));

        // Valid comparison
        let expr = Expression::gt(Expression::column(0), Expression::literal(Value::Int32(10)));
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Boolean));

        // Valid logical operation
        let expr = Expression::and(
            Expression::column(2),
            Expression::literal(Value::Boolean(true)),
        );
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Boolean));

        // Invalid logical operation
        let expr = Expression::and(Expression::column(0), Expression::column(2));
        assert!(matches!(
            checker.check(&expr),
            Err(ExpressionError::InvalidOperandTypes { .. })
        ));

        // NULL handling
        let expr = Expression::add_expr(Expression::column(0), Expression::literal(Value::Null));
        assert_eq!(checker.check(&expr).unwrap(), None);
    }

    #[test]
    fn test_unary_op_type_checking() {
        let schema = vec![DataType::Int32, DataType::Boolean];
        let checker = TypeChecker::new(&schema);

        // Valid NOT
        let expr = Expression::not_expr(Expression::column(1));
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Boolean));

        // Invalid NOT
        let expr = Expression::not_expr(Expression::column(0));
        assert!(matches!(
            checker.check(&expr),
            Err(ExpressionError::InvalidOperandTypes { .. })
        ));

        // IS NULL works on any type
        let expr = Expression::is_null(Expression::column(0));
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Boolean));

        let expr = Expression::is_not_null(Expression::column(1));
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Boolean));

        // Unary arithmetic
        let expr = Expression::unary_op(UnaryOperator::Minus, Expression::column(0));
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Int32));

        let expr = Expression::unary_op(UnaryOperator::Minus, Expression::column(1));
        assert!(matches!(
            checker.check(&expr),
            Err(ExpressionError::InvalidOperandTypes { .. })
        ));
    }

    #[test]
    fn test_filter_predicate_checking() {
        let schema = vec![DataType::Int32, DataType::Boolean];
        let checker = TypeChecker::new(&schema);

        // Valid boolean expression
        let expr = Expression::gt(Expression::column(0), Expression::literal(Value::Int32(5)));
        assert!(checker.check_filter_predicate(&expr).is_ok());

        // Direct boolean column
        let expr = Expression::column(1);
        assert!(checker.check_filter_predicate(&expr).is_ok());

        // NULL is allowed in filters
        let expr = Expression::literal(Value::Null);
        assert!(checker.check_filter_predicate(&expr).is_ok());

        // Non-boolean expression
        let expr = Expression::column(0);
        assert!(matches!(
            checker.check_filter_predicate(&expr),
            Err(ExpressionError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn test_complex_expression_type_checking() {
        let schema = vec![DataType::Int32, DataType::Int32, DataType::Boolean];
        let checker = TypeChecker::new(&schema);

        // (col0 + col1) > 10 AND col2
        let expr = Expression::and(
            Expression::gt(
                Expression::add_expr(Expression::column(0), Expression::column(1)),
                Expression::literal(Value::Int32(10)),
            ),
            Expression::column(2),
        );
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Boolean));

        // Nested arithmetic: (col0 + 5) * (col1 - 3)
        let expr = Expression::mul_expr(
            Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(5))),
            Expression::sub_expr(Expression::column(1), Expression::literal(Value::Int32(3))),
        );
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Int32));
    }

    #[test]
    fn test_type_compatibility() {
        let schema = vec![DataType::Int32, DataType::Varchar];
        let checker = TypeChecker::new(&schema);

        // Same types are compatible
        let expr1 = Expression::column(0);
        let expr2 = Expression::literal(Value::Int32(42));
        assert!(checker.check_compatible(&expr1, &expr2).is_ok());

        // Different types are not compatible
        let expr1 = Expression::column(0);
        let expr2 = Expression::column(1);
        assert!(matches!(
            checker.check_compatible(&expr1, &expr2),
            Err(ExpressionError::TypeMismatch { .. })
        ));

        // NULL is compatible with any type
        let expr1 = Expression::column(0);
        let expr2 = Expression::literal(Value::Null);
        assert!(checker.check_compatible(&expr1, &expr2).is_ok());
    }

    #[test]
    fn test_string_operations_type_checking() {
        let schema = vec![DataType::Varchar];
        let checker = TypeChecker::new(&schema);

        // String concatenation
        let expr = Expression::binary_op(
            BinaryOperator::Concat,
            Expression::column(0),
            Expression::literal(Value::String("suffix".to_string())),
        );
        assert_eq!(checker.check(&expr).unwrap(), Some(DataType::Varchar));

        // Invalid concatenation
        let expr = Expression::binary_op(
            BinaryOperator::Concat,
            Expression::column(0),
            Expression::literal(Value::Int32(42)),
        );
        assert!(matches!(
            checker.check(&expr),
            Err(ExpressionError::InvalidOperandTypes { .. })
        ));
    }
}
