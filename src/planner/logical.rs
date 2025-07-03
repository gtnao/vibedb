//! Logical query plan representation.
//!
//! Logical plans are high-level, optimizer-friendly representations of queries.
//! They abstract away physical implementation details and focus on the logical
//! operations needed to produce the query result.

use crate::access::Value;
use crate::sql::ast::{ColumnDefinition, DataType, TableConstraint};

/// Root logical plan types
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// SELECT query
    Select(Box<LogicalPlanNode>),

    /// INSERT statement
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<LogicalExpression>>,
    },

    /// UPDATE statement
    Update {
        table: String,
        assignments: Vec<(String, LogicalExpression)>,
        filter: Option<Box<LogicalPlanNode>>,
    },

    /// DELETE statement
    Delete {
        table: String,
        filter: Option<Box<LogicalPlanNode>>,
    },

    /// CREATE TABLE statement
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
        constraints: Vec<TableConstraint>,
    },

    /// BEGIN TRANSACTION
    BeginTransaction,

    /// COMMIT TRANSACTION
    Commit,

    /// ROLLBACK TRANSACTION
    Rollback,
}

/// Logical plan node for query trees
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlanNode {
    /// Scan a table
    TableScan {
        table_name: String,
        alias: Option<String>,
    },

    /// Filter rows based on predicate
    Filter {
        input: Box<LogicalPlanNode>,
        predicate: LogicalExpression,
    },

    /// Project specific columns
    Projection {
        input: Box<LogicalPlanNode>,
        expressions: Vec<(LogicalExpression, Option<String>)>, // (expr, alias)
    },

    /// Sort rows
    Sort {
        input: Box<LogicalPlanNode>,
        exprs: Vec<(LogicalExpression, bool)>, // (expr, is_ascending)
    },

    /// Limit rows
    Limit {
        input: Box<LogicalPlanNode>,
        limit: Option<LogicalExpression>,
        offset: Option<LogicalExpression>,
    },

    /// Join two relations
    Join {
        left: Box<LogicalPlanNode>,
        right: Box<LogicalPlanNode>,
        join_type: JoinType,
        condition: Option<LogicalExpression>,
    },

    /// Group by with aggregation
    GroupBy {
        input: Box<LogicalPlanNode>,
        group_exprs: Vec<LogicalExpression>,
        aggregate_exprs: Vec<AggregateExpression>,
    },

    /// Filter after aggregation (HAVING)
    Having {
        input: Box<LogicalPlanNode>,
        predicate: LogicalExpression,
    },

    /// Remove duplicate rows
    Distinct { input: Box<LogicalPlanNode> },

    /// Literal values (for SELECT without FROM)
    Values { rows: Vec<Vec<LogicalExpression>> },
}

/// Logical expression representation
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpression {
    /// Literal value
    Literal(Value),

    /// NULL literal
    Null,

    /// Column reference
    Column(String),

    /// Qualified column reference (table.column)
    QualifiedColumn(String, String),

    /// Binary operation
    BinaryOp {
        left: Box<LogicalExpression>,
        op: BinaryOp,
        right: Box<LogicalExpression>,
    },

    /// Unary operation
    UnaryOp {
        op: UnaryOp,
        operand: Box<LogicalExpression>,
    },

    /// Function call
    Function {
        name: String,
        args: Vec<LogicalExpression>,
        distinct: bool,
    },

    /// Cast expression
    Cast {
        expression: Box<LogicalExpression>,
        data_type: DataType,
    },

    /// IS NULL test
    IsNull {
        expression: Box<LogicalExpression>,
        negated: bool,
    },

    /// IN list
    InList {
        expression: Box<LogicalExpression>,
        list: Vec<LogicalExpression>,
        negated: bool,
    },

    /// BETWEEN
    Between {
        expression: Box<LogicalExpression>,
        low: Box<LogicalExpression>,
        high: Box<LogicalExpression>,
        negated: bool,
    },

    /// LIKE pattern matching
    Like {
        expression: Box<LogicalExpression>,
        pattern: Box<LogicalExpression>,
        escape: Option<Box<LogicalExpression>>,
        negated: bool,
    },
}

/// Aggregate expression
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpression {
    pub function: String,
    pub args: Vec<LogicalExpression>,
    pub distinct: bool,
    pub alias: Option<String>,
}

/// Aggregate functions
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Join types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,

    // Logical
    And,
    Or,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOp {
    Not,
    Minus,
    Plus,
}

impl LogicalExpression {
    /// Create a literal expression
    pub fn literal(value: Value) -> Self {
        LogicalExpression::Literal(value)
    }

    /// Create a column reference
    pub fn column(name: impl Into<String>) -> Self {
        LogicalExpression::Column(name.into())
    }

    /// Create an equality comparison
    pub fn eq(self, other: LogicalExpression) -> Self {
        LogicalExpression::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    /// Create an AND expression
    pub fn and(self, other: LogicalExpression) -> Self {
        LogicalExpression::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    /// Create an OR expression
    pub fn or(self, other: LogicalExpression) -> Self {
        LogicalExpression::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_expression_builders() {
        let expr = LogicalExpression::column("age")
            .eq(LogicalExpression::literal(Value::Int32(21)))
            .and(
                LogicalExpression::column("active")
                    .eq(LogicalExpression::literal(Value::Boolean(true))),
            );

        match expr {
            LogicalExpression::BinaryOp {
                op: BinaryOp::And, ..
            } => {
                // Success
            }
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_logical_plan_construction() {
        // SELECT name FROM users WHERE age > 18
        let plan = LogicalPlan::Select(Box::new(LogicalPlanNode::Projection {
            input: Box::new(LogicalPlanNode::Filter {
                input: Box::new(LogicalPlanNode::TableScan {
                    table_name: "users".to_string(),
                    alias: None,
                }),
                predicate: LogicalExpression::BinaryOp {
                    left: Box::new(LogicalExpression::column("age")),
                    op: BinaryOp::Gt,
                    right: Box::new(LogicalExpression::literal(Value::Int32(18))),
                },
            }),
            expressions: vec![(LogicalExpression::column("name"), None)],
        }));

        match plan {
            LogicalPlan::Select(node) => {
                match node.as_ref() {
                    LogicalPlanNode::Projection { .. } => {
                        // Success
                    }
                    _ => panic!("Expected Projection node"),
                }
            }
            _ => panic!("Expected Select plan"),
        }
    }

    #[test]
    fn test_join_node() {
        let left = Box::new(LogicalPlanNode::TableScan {
            table_name: "users".to_string(),
            alias: Some("u".to_string()),
        });

        let right = Box::new(LogicalPlanNode::TableScan {
            table_name: "orders".to_string(),
            alias: Some("o".to_string()),
        });

        let join = LogicalPlanNode::Join {
            left,
            right,
            join_type: JoinType::Inner,
            condition: Some(LogicalExpression::BinaryOp {
                left: Box::new(LogicalExpression::QualifiedColumn(
                    "u".to_string(),
                    "id".to_string(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpression::QualifiedColumn(
                    "o".to_string(),
                    "user_id".to_string(),
                )),
            }),
        };

        match join {
            LogicalPlanNode::Join {
                join_type: JoinType::Inner,
                ..
            } => {
                // Success
            }
            _ => panic!("Expected Inner Join"),
        }
    }
}
