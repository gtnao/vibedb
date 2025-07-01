//! Operator definitions for expressions.

use crate::access::DataType;

/// Binary operators supported in expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,

    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,

    // Logical
    And,
    Or,

    // String operators (future)
    Concat,
    Like,
}

impl BinaryOperator {
    /// Get the output type of this operator given input types
    pub fn output_type(&self, left: DataType, right: DataType) -> Option<DataType> {
        match self {
            // Arithmetic operators
            BinaryOperator::Add
            | BinaryOperator::Sub
            | BinaryOperator::Mul
            | BinaryOperator::Div => {
                match (left, right) {
                    (DataType::Int32, DataType::Int32) => Some(DataType::Int32),
                    _ => None, // Type mismatch or unsupported types
                }
            }

            // Comparison operators always return boolean
            BinaryOperator::Eq
            | BinaryOperator::Ne
            | BinaryOperator::Lt
            | BinaryOperator::Le
            | BinaryOperator::Gt
            | BinaryOperator::Ge => {
                // Check if types are compatible for comparison
                if self.types_compatible_for_comparison(left, right) {
                    Some(DataType::Boolean)
                } else {
                    None
                }
            }

            // Logical operators
            BinaryOperator::And | BinaryOperator::Or => match (left, right) {
                (DataType::Boolean, DataType::Boolean) => Some(DataType::Boolean),
                _ => None,
            },

            // String operators
            BinaryOperator::Concat => match (left, right) {
                (DataType::Varchar, DataType::Varchar) => Some(DataType::Varchar),
                _ => None,
            },

            BinaryOperator::Like => match (left, right) {
                (DataType::Varchar, DataType::Varchar) => Some(DataType::Boolean),
                _ => None,
            },
        }
    }

    /// Check if two types are compatible for comparison
    fn types_compatible_for_comparison(&self, left: DataType, right: DataType) -> bool {
        // Same type is always comparable
        if left == right {
            return true;
        }

        // For now, we only allow comparison between same types
        // In the future, we might allow Int32 vs Float comparisons, etc.
        false
    }

    /// Get the display string for this operator
    pub fn as_str(&self) -> &'static str {
        match self {
            BinaryOperator::Add => "+",
            BinaryOperator::Sub => "-",
            BinaryOperator::Mul => "*",
            BinaryOperator::Div => "/",
            BinaryOperator::Eq => "=",
            BinaryOperator::Ne => "!=",
            BinaryOperator::Lt => "<",
            BinaryOperator::Le => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::Ge => ">=",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Concat => "||",
            BinaryOperator::Like => "LIKE",
        }
    }
}

/// Unary operators supported in expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    // Logical
    Not,

    // NULL checks
    IsNull,
    IsNotNull,

    // Arithmetic (future)
    Plus,
    Minus,
}

impl UnaryOperator {
    /// Get the output type of this operator given input type
    pub fn output_type(&self, operand: DataType) -> Option<DataType> {
        match self {
            UnaryOperator::Not => match operand {
                DataType::Boolean => Some(DataType::Boolean),
                _ => None,
            },

            // NULL checks always return boolean regardless of input type
            UnaryOperator::IsNull | UnaryOperator::IsNotNull => Some(DataType::Boolean),

            UnaryOperator::Plus | UnaryOperator::Minus => match operand {
                DataType::Int32 => Some(DataType::Int32),
                _ => None,
            },
        }
    }

    /// Get the display string for this operator
    pub fn as_str(&self) -> &'static str {
        match self {
            UnaryOperator::Not => "NOT",
            UnaryOperator::IsNull => "IS NULL",
            UnaryOperator::IsNotNull => "IS NOT NULL",
            UnaryOperator::Plus => "+",
            UnaryOperator::Minus => "-",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_operator_output_types() {
        // Arithmetic operators
        assert_eq!(
            BinaryOperator::Add.output_type(DataType::Int32, DataType::Int32),
            Some(DataType::Int32)
        );
        assert_eq!(
            BinaryOperator::Sub.output_type(DataType::Int32, DataType::Int32),
            Some(DataType::Int32)
        );
        assert_eq!(
            BinaryOperator::Mul.output_type(DataType::Int32, DataType::Int32),
            Some(DataType::Int32)
        );
        assert_eq!(
            BinaryOperator::Div.output_type(DataType::Int32, DataType::Int32),
            Some(DataType::Int32)
        );

        // Type mismatch for arithmetic
        assert_eq!(
            BinaryOperator::Add.output_type(DataType::Int32, DataType::Varchar),
            None
        );
        assert_eq!(
            BinaryOperator::Add.output_type(DataType::Varchar, DataType::Varchar),
            None
        );

        // Comparison operators
        assert_eq!(
            BinaryOperator::Eq.output_type(DataType::Int32, DataType::Int32),
            Some(DataType::Boolean)
        );
        assert_eq!(
            BinaryOperator::Lt.output_type(DataType::Varchar, DataType::Varchar),
            Some(DataType::Boolean)
        );
        assert_eq!(
            BinaryOperator::Ge.output_type(DataType::Boolean, DataType::Boolean),
            Some(DataType::Boolean)
        );

        // Type mismatch for comparison
        assert_eq!(
            BinaryOperator::Eq.output_type(DataType::Int32, DataType::Varchar),
            None
        );

        // Logical operators
        assert_eq!(
            BinaryOperator::And.output_type(DataType::Boolean, DataType::Boolean),
            Some(DataType::Boolean)
        );
        assert_eq!(
            BinaryOperator::Or.output_type(DataType::Boolean, DataType::Boolean),
            Some(DataType::Boolean)
        );

        // Type mismatch for logical
        assert_eq!(
            BinaryOperator::And.output_type(DataType::Int32, DataType::Boolean),
            None
        );

        // String operators
        assert_eq!(
            BinaryOperator::Concat.output_type(DataType::Varchar, DataType::Varchar),
            Some(DataType::Varchar)
        );
        assert_eq!(
            BinaryOperator::Like.output_type(DataType::Varchar, DataType::Varchar),
            Some(DataType::Boolean)
        );
    }

    #[test]
    fn test_unary_operator_output_types() {
        // Logical NOT
        assert_eq!(
            UnaryOperator::Not.output_type(DataType::Boolean),
            Some(DataType::Boolean)
        );
        assert_eq!(UnaryOperator::Not.output_type(DataType::Int32), None);

        // NULL checks work on any type
        assert_eq!(
            UnaryOperator::IsNull.output_type(DataType::Int32),
            Some(DataType::Boolean)
        );
        assert_eq!(
            UnaryOperator::IsNull.output_type(DataType::Varchar),
            Some(DataType::Boolean)
        );
        assert_eq!(
            UnaryOperator::IsNull.output_type(DataType::Boolean),
            Some(DataType::Boolean)
        );
        assert_eq!(
            UnaryOperator::IsNotNull.output_type(DataType::Int32),
            Some(DataType::Boolean)
        );

        // Arithmetic unary
        assert_eq!(
            UnaryOperator::Plus.output_type(DataType::Int32),
            Some(DataType::Int32)
        );
        assert_eq!(
            UnaryOperator::Minus.output_type(DataType::Int32),
            Some(DataType::Int32)
        );
        assert_eq!(UnaryOperator::Plus.output_type(DataType::Varchar), None);
    }

    #[test]
    fn test_operator_display() {
        // Binary operators
        assert_eq!(BinaryOperator::Add.as_str(), "+");
        assert_eq!(BinaryOperator::Eq.as_str(), "=");
        assert_eq!(BinaryOperator::Ne.as_str(), "!=");
        assert_eq!(BinaryOperator::And.as_str(), "AND");
        assert_eq!(BinaryOperator::Concat.as_str(), "||");

        // Unary operators
        assert_eq!(UnaryOperator::Not.as_str(), "NOT");
        assert_eq!(UnaryOperator::IsNull.as_str(), "IS NULL");
        assert_eq!(UnaryOperator::IsNotNull.as_str(), "IS NOT NULL");
    }
}
