//! Error types for expression evaluation.

use crate::access::DataType;
use std::fmt;

/// Errors that can occur during expression evaluation
#[derive(Debug, Clone, PartialEq)]
pub enum ExpressionError {
    /// Type mismatch in operation
    TypeMismatch {
        expected: DataType,
        actual: DataType,
        context: String,
    },

    /// Invalid operand types for operator
    InvalidOperandTypes {
        operator: String,
        left_type: Option<DataType>,
        right_type: Option<DataType>,
    },

    /// Column index out of bounds
    ColumnIndexOutOfBounds { index: usize, tuple_size: usize },

    /// Division by zero
    DivisionByZero,

    /// NULL value in non-nullable context
    UnexpectedNull { context: String },

    /// Invalid function name
    UnknownFunction { name: String },

    /// Wrong number of function arguments
    FunctionArgumentCount {
        function: String,
        expected: usize,
        actual: usize,
    },

    /// Generic evaluation error
    EvaluationError { message: String },

    /// Type checking failed
    TypeCheckFailed { expression: String, reason: String },
}

impl fmt::Display for ExpressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionError::TypeMismatch {
                expected,
                actual,
                context,
            } => {
                write!(
                    f,
                    "Type mismatch in {}: expected {:?}, got {:?}",
                    context, expected, actual
                )
            }

            ExpressionError::InvalidOperandTypes {
                operator,
                left_type,
                right_type,
            } => {
                write!(
                    f,
                    "Invalid operand types for operator {}: left={:?}, right={:?}",
                    operator, left_type, right_type
                )
            }

            ExpressionError::ColumnIndexOutOfBounds { index, tuple_size } => {
                write!(
                    f,
                    "Column index {} out of bounds for tuple with {} columns",
                    index, tuple_size
                )
            }

            ExpressionError::DivisionByZero => write!(f, "Division by zero"),

            ExpressionError::UnexpectedNull { context } => {
                write!(f, "Unexpected NULL value in {}", context)
            }

            ExpressionError::UnknownFunction { name } => {
                write!(f, "Unknown function: {}", name)
            }

            ExpressionError::FunctionArgumentCount {
                function,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Function {} expects {} arguments, got {}",
                    function, expected, actual
                )
            }

            ExpressionError::EvaluationError { message } => {
                write!(f, "Expression evaluation error: {}", message)
            }

            ExpressionError::TypeCheckFailed { expression, reason } => {
                write!(
                    f,
                    "Type check failed for expression '{}': {}",
                    expression, reason
                )
            }
        }
    }
}

impl std::error::Error for ExpressionError {}

/// Result type for expression operations
pub type ExpressionResult<T> = Result<T, ExpressionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ExpressionError::TypeMismatch {
            expected: DataType::Int32,
            actual: DataType::Varchar,
            context: "addition".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Type mismatch in addition: expected Int32, got Varchar"
        );

        let err = ExpressionError::InvalidOperandTypes {
            operator: "+".to_string(),
            left_type: Some(DataType::Int32),
            right_type: Some(DataType::Varchar),
        };
        assert_eq!(
            err.to_string(),
            "Invalid operand types for operator +: left=Some(Int32), right=Some(Varchar)"
        );

        let err = ExpressionError::ColumnIndexOutOfBounds {
            index: 5,
            tuple_size: 3,
        };
        assert_eq!(
            err.to_string(),
            "Column index 5 out of bounds for tuple with 3 columns"
        );

        let err = ExpressionError::DivisionByZero;
        assert_eq!(err.to_string(), "Division by zero");

        let err = ExpressionError::UnexpectedNull {
            context: "arithmetic operation".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Unexpected NULL value in arithmetic operation"
        );

        let err = ExpressionError::UnknownFunction {
            name: "foo".to_string(),
        };
        assert_eq!(err.to_string(), "Unknown function: foo");

        let err = ExpressionError::FunctionArgumentCount {
            function: "substring".to_string(),
            expected: 3,
            actual: 2,
        };
        assert_eq!(
            err.to_string(),
            "Function substring expects 3 arguments, got 2"
        );
    }
}
