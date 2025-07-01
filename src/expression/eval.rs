//! Expression evaluation implementation.

use crate::access::{DataType, Value};
use crate::expression::{
    BinaryOperator, ColumnRef, Expression, ExpressionError, ExpressionResult, UnaryOperator,
};

/// Evaluator for expressions
pub struct ExpressionEvaluator<'a> {
    /// The tuple values to evaluate against
    tuple_values: &'a [Value],
    /// Optional schema for type checking
    #[allow(dead_code)]
    schema: Option<&'a [DataType]>,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new evaluator with tuple values
    pub fn new(tuple_values: &'a [Value]) -> Self {
        Self {
            tuple_values,
            schema: None,
        }
    }

    /// Create a new evaluator with tuple values and schema
    pub fn with_schema(tuple_values: &'a [Value], schema: &'a [DataType]) -> Self {
        Self {
            tuple_values,
            schema: Some(schema),
        }
    }

    /// Evaluate an expression and return the result
    pub fn evaluate(&self, expr: &Expression) -> ExpressionResult<Value> {
        match expr {
            Expression::Literal(lit) => Ok(lit.value.clone()),

            Expression::ColumnRef(col) => self.evaluate_column_ref(col),

            Expression::BinaryOp { op, left, right } => {
                let left_val = self.evaluate(left)?;
                let right_val = self.evaluate(right)?;
                self.evaluate_binary_op(*op, left_val, right_val)
            }

            Expression::UnaryOp { op, operand } => {
                let operand_val = self.evaluate(operand)?;
                self.evaluate_unary_op(*op, operand_val)
            }

            Expression::FunctionCall { name, .. } => {
                Err(ExpressionError::UnknownFunction { name: name.clone() })
            }

            Expression::Case { .. } => Err(ExpressionError::EvaluationError {
                message: "CASE expressions not yet implemented".to_string(),
            }),

            Expression::In { .. } => Err(ExpressionError::EvaluationError {
                message: "IN expressions not yet implemented".to_string(),
            }),

            Expression::Between { .. } => Err(ExpressionError::EvaluationError {
                message: "BETWEEN expressions not yet implemented".to_string(),
            }),
        }
    }

    /// Evaluate a column reference
    fn evaluate_column_ref(&self, col: &ColumnRef) -> ExpressionResult<Value> {
        if col.index >= self.tuple_values.len() {
            return Err(ExpressionError::ColumnIndexOutOfBounds {
                index: col.index,
                tuple_size: self.tuple_values.len(),
            });
        }
        Ok(self.tuple_values[col.index].clone())
    }

    /// Evaluate a binary operation
    fn evaluate_binary_op(
        &self,
        op: BinaryOperator,
        left: Value,
        right: Value,
    ) -> ExpressionResult<Value> {
        // Handle NULL propagation for most operators
        match (&left, &right) {
            (Value::Null, _) | (_, Value::Null) => {
                // For comparisons with NULL, result is NULL (three-valued logic)
                match op {
                    BinaryOperator::Eq
                    | BinaryOperator::Ne
                    | BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge => return Ok(Value::Null),
                    // For AND/OR, special NULL handling
                    BinaryOperator::And => {
                        // NULL AND false = false, NULL AND true = NULL
                        match (&left, &right) {
                            (Value::Boolean(false), _) | (_, Value::Boolean(false)) => {
                                return Ok(Value::Boolean(false))
                            }
                            _ => return Ok(Value::Null),
                        }
                    }
                    BinaryOperator::Or => {
                        // NULL OR true = true, NULL OR false = NULL
                        match (&left, &right) {
                            (Value::Boolean(true), _) | (_, Value::Boolean(true)) => {
                                return Ok(Value::Boolean(true))
                            }
                            _ => return Ok(Value::Null),
                        }
                    }
                    // For arithmetic and other operators, NULL propagates
                    _ => return Ok(Value::Null),
                }
            }
            _ => {}
        }

        match op {
            // Arithmetic operators
            BinaryOperator::Add => match (&left, &right) {
                (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a.wrapping_add(*b))),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            BinaryOperator::Sub => match (&left, &right) {
                (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a.wrapping_sub(*b))),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            BinaryOperator::Mul => match (&left, &right) {
                (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a.wrapping_mul(*b))),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            BinaryOperator::Div => match (&left, &right) {
                (Value::Int32(a), Value::Int32(b)) => {
                    if *b == 0 {
                        Err(ExpressionError::DivisionByZero)
                    } else {
                        Ok(Value::Int32(a / b))
                    }
                }
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            // Comparison operators
            BinaryOperator::Eq => self.compare_values(left, right, |cmp| cmp == 0),
            BinaryOperator::Ne => self.compare_values(left, right, |cmp| cmp != 0),
            BinaryOperator::Lt => self.compare_values(left, right, |cmp| cmp < 0),
            BinaryOperator::Le => self.compare_values(left, right, |cmp| cmp <= 0),
            BinaryOperator::Gt => self.compare_values(left, right, |cmp| cmp > 0),
            BinaryOperator::Ge => self.compare_values(left, right, |cmp| cmp >= 0),

            // Logical operators
            BinaryOperator::And => match (&left, &right) {
                (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a && *b)),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            BinaryOperator::Or => match (&left, &right) {
                (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a || *b)),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            // String operators
            BinaryOperator::Concat => match (&left, &right) {
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                }),
            },

            BinaryOperator::Like => Err(ExpressionError::EvaluationError {
                message: "LIKE operator not yet implemented".to_string(),
            }),
        }
    }

    /// Evaluate a unary operation
    fn evaluate_unary_op(&self, op: UnaryOperator, operand: Value) -> ExpressionResult<Value> {
        match op {
            UnaryOperator::Not => match operand {
                Value::Null => Ok(Value::Null),
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: operand.data_type(),
                    right_type: None,
                }),
            },

            UnaryOperator::IsNull => Ok(Value::Boolean(matches!(operand, Value::Null))),

            UnaryOperator::IsNotNull => Ok(Value::Boolean(!matches!(operand, Value::Null))),

            UnaryOperator::Plus => match operand {
                Value::Null => Ok(Value::Null),
                Value::Int32(n) => Ok(Value::Int32(n)),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: operand.data_type(),
                    right_type: None,
                }),
            },

            UnaryOperator::Minus => match operand {
                Value::Null => Ok(Value::Null),
                Value::Int32(n) => Ok(Value::Int32(-n)),
                _ => Err(ExpressionError::InvalidOperandTypes {
                    operator: op.as_str().to_string(),
                    left_type: operand.data_type(),
                    right_type: None,
                }),
            },
        }
    }

    /// Compare two values and apply a comparison function
    fn compare_values<F>(&self, left: Value, right: Value, cmp_fn: F) -> ExpressionResult<Value>
    where
        F: FnOnce(i32) -> bool,
    {
        let cmp_result = match (&left, &right) {
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            _ => {
                return Err(ExpressionError::InvalidOperandTypes {
                    operator: "comparison".to_string(),
                    left_type: left.data_type(),
                    right_type: right.data_type(),
                })
            }
        };

        Ok(Value::Boolean(cmp_fn(match cmp_result {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        })))
    }
}

/// Helper function to evaluate an expression against tuple values
pub fn evaluate_expression(expr: &Expression, tuple_values: &[Value]) -> ExpressionResult<Value> {
    ExpressionEvaluator::new(tuple_values).evaluate(expr)
}

/// Type alias for predicate functions
pub type Predicate = Box<dyn Fn(&[Value]) -> bool + Send + 'static>;

/// Helper function to create a predicate function from an expression
pub fn expression_to_predicate(expr: Expression) -> Predicate {
    Box::new(move |values| {
        match evaluate_expression(&expr, values) {
            Ok(Value::Boolean(b)) => b,
            Ok(Value::Null) => false, // NULL is treated as false in WHERE clause
            _ => false,               // Type error or non-boolean result
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_evaluation() {
        let values = vec![];
        let evaluator = ExpressionEvaluator::new(&values);

        // Test various literals
        assert_eq!(
            evaluator
                .evaluate(&Expression::literal(Value::Int32(42)))
                .unwrap(),
            Value::Int32(42)
        );
        assert_eq!(
            evaluator
                .evaluate(&Expression::literal(Value::Boolean(true)))
                .unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            evaluator
                .evaluate(&Expression::literal(Value::String("hello".to_string())))
                .unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(
            evaluator
                .evaluate(&Expression::literal(Value::Null))
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_column_ref_evaluation() {
        let values = vec![
            Value::Int32(1),
            Value::String("test".to_string()),
            Value::Boolean(true),
        ];
        let evaluator = ExpressionEvaluator::new(&values);

        assert_eq!(
            evaluator.evaluate(&Expression::column(0)).unwrap(),
            Value::Int32(1)
        );
        assert_eq!(
            evaluator.evaluate(&Expression::column(1)).unwrap(),
            Value::String("test".to_string())
        );
        assert_eq!(
            evaluator.evaluate(&Expression::column(2)).unwrap(),
            Value::Boolean(true)
        );

        // Out of bounds
        assert!(matches!(
            evaluator.evaluate(&Expression::column(3)),
            Err(ExpressionError::ColumnIndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn test_arithmetic_operations() {
        let values = vec![];
        let evaluator = ExpressionEvaluator::new(&values);

        // Addition
        let expr = Expression::add_expr(
            Expression::literal(Value::Int32(10)),
            Expression::literal(Value::Int32(5)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Int32(15));

        // Subtraction
        let expr = Expression::sub_expr(
            Expression::literal(Value::Int32(10)),
            Expression::literal(Value::Int32(5)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Int32(5));

        // Multiplication
        let expr = Expression::mul_expr(
            Expression::literal(Value::Int32(4)),
            Expression::literal(Value::Int32(3)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Int32(12));

        // Division
        let expr = Expression::div_expr(
            Expression::literal(Value::Int32(10)),
            Expression::literal(Value::Int32(3)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Int32(3));

        // Division by zero
        let expr = Expression::div_expr(
            Expression::literal(Value::Int32(10)),
            Expression::literal(Value::Int32(0)),
        );
        assert!(matches!(
            evaluator.evaluate(&expr),
            Err(ExpressionError::DivisionByZero)
        ));

        // Type mismatch
        let expr = Expression::add_expr(
            Expression::literal(Value::Int32(10)),
            Expression::literal(Value::String("5".to_string())),
        );
        assert!(matches!(
            evaluator.evaluate(&expr),
            Err(ExpressionError::InvalidOperandTypes { .. })
        ));
    }

    #[test]
    fn test_comparison_operations() {
        let values = vec![];
        let evaluator = ExpressionEvaluator::new(&values);

        // Integer comparisons
        let expr = Expression::eq(
            Expression::literal(Value::Int32(5)),
            Expression::literal(Value::Int32(5)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::ne(
            Expression::literal(Value::Int32(5)),
            Expression::literal(Value::Int32(3)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::lt(
            Expression::literal(Value::Int32(3)),
            Expression::literal(Value::Int32(5)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::ge(
            Expression::literal(Value::Int32(5)),
            Expression::literal(Value::Int32(5)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        // String comparisons
        let expr = Expression::eq(
            Expression::literal(Value::String("abc".to_string())),
            Expression::literal(Value::String("abc".to_string())),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::lt(
            Expression::literal(Value::String("abc".to_string())),
            Expression::literal(Value::String("def".to_string())),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        // Boolean comparisons
        let expr = Expression::eq(
            Expression::literal(Value::Boolean(true)),
            Expression::literal(Value::Boolean(true)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_logical_operations() {
        let values = vec![];
        let evaluator = ExpressionEvaluator::new(&values);

        // AND
        let expr = Expression::and(
            Expression::literal(Value::Boolean(true)),
            Expression::literal(Value::Boolean(true)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::and(
            Expression::literal(Value::Boolean(true)),
            Expression::literal(Value::Boolean(false)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(false));

        // OR
        let expr = Expression::or(
            Expression::literal(Value::Boolean(false)),
            Expression::literal(Value::Boolean(true)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::or(
            Expression::literal(Value::Boolean(false)),
            Expression::literal(Value::Boolean(false)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(false));

        // NOT
        let expr = Expression::not_expr(Expression::literal(Value::Boolean(true)));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(false));

        let expr = Expression::not_expr(Expression::literal(Value::Boolean(false)));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_null_operations() {
        let values = vec![Value::Null, Value::Int32(5)];
        let evaluator = ExpressionEvaluator::new(&values);

        // IS NULL
        let expr = Expression::is_null(Expression::column(0));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        let expr = Expression::is_null(Expression::column(1));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(false));

        // IS NOT NULL
        let expr = Expression::is_not_null(Expression::column(0));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(false));

        let expr = Expression::is_not_null(Expression::column(1));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        // NULL propagation in arithmetic
        let expr =
            Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(5)));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Null);

        // NULL in comparisons
        let expr = Expression::eq(Expression::column(0), Expression::literal(Value::Int32(5)));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Null);

        // NULL in AND (special case: false AND NULL = false)
        let expr = Expression::and(
            Expression::literal(Value::Boolean(false)),
            Expression::column(0),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(false));

        // NULL in OR (special case: true OR NULL = true)
        let expr = Expression::or(
            Expression::literal(Value::Boolean(true)),
            Expression::column(0),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_complex_expressions() {
        let values = vec![Value::Int32(10), Value::Int32(5), Value::Boolean(true)];
        let evaluator = ExpressionEvaluator::new(&values);

        // (col0 + col1) > 12 AND col2
        let expr = Expression::and(
            Expression::gt(
                Expression::add_expr(Expression::column(0), Expression::column(1)),
                Expression::literal(Value::Int32(12)),
            ),
            Expression::column(2),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));

        // (col0 - col1) * 2 = 10
        let expr = Expression::eq(
            Expression::mul_expr(
                Expression::sub_expr(Expression::column(0), Expression::column(1)),
                Expression::literal(Value::Int32(2)),
            ),
            Expression::literal(Value::Int32(10)),
        );
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_string_operations() {
        let values = vec![];
        let evaluator = ExpressionEvaluator::new(&values);

        // String concatenation
        let expr = Expression::binary_op(
            BinaryOperator::Concat,
            Expression::literal(Value::String("Hello".to_string())),
            Expression::literal(Value::String(" World".to_string())),
        );
        assert_eq!(
            evaluator.evaluate(&expr).unwrap(),
            Value::String("Hello World".to_string())
        );
    }

    #[test]
    fn test_expression_to_predicate() {
        // Test simple boolean expression
        let expr = Expression::gt(Expression::column(0), Expression::literal(Value::Int32(5)));
        let predicate = expression_to_predicate(expr);

        assert!(predicate(&[Value::Int32(10)]));
        assert!(!predicate(&[Value::Int32(3)]));

        // Test NULL handling in predicate
        let expr = Expression::eq(Expression::column(0), Expression::literal(Value::Int32(5)));
        let predicate = expression_to_predicate(expr);

        assert!(!predicate(&[Value::Null])); // NULL = 5 evaluates to NULL, which is false in WHERE

        // Test type error handling
        let expr = Expression::and(
            Expression::column(0),
            Expression::literal(Value::Boolean(true)),
        );
        let predicate = expression_to_predicate(expr);

        assert!(!predicate(&[Value::Int32(10)])); // Type error results in false
    }

    #[test]
    fn test_unary_arithmetic() {
        let values = vec![];
        let evaluator = ExpressionEvaluator::new(&values);

        // Unary plus
        let expr = Expression::unary_op(UnaryOperator::Plus, Expression::literal(Value::Int32(42)));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Int32(42));

        // Unary minus
        let expr =
            Expression::unary_op(UnaryOperator::Minus, Expression::literal(Value::Int32(42)));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Int32(-42));

        // Unary on NULL
        let expr = Expression::unary_op(UnaryOperator::Minus, Expression::literal(Value::Null));
        assert_eq!(evaluator.evaluate(&expr).unwrap(), Value::Null);
    }
}
