//! Physical query plan representation.
//!
//! Physical plans are concrete execution plans that map directly to executor
//! implementations. They specify exactly how to execute the query, including
//! which algorithms to use (e.g., hash join vs nested loop join).

use crate::planner::logical::{AggregateExpression, JoinType, LogicalExpression};
use crate::sql::ast::{ColumnDefinition, TableConstraint};

/// Root physical plan types
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// SELECT query execution
    Select(Box<PhysicalPlanNode>),

    /// INSERT execution
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<LogicalExpression>>,
    },

    /// UPDATE execution
    Update {
        table: String,
        assignments: Vec<(String, LogicalExpression)>,
        filter: Option<Box<PhysicalPlanNode>>,
    },

    /// DELETE execution
    Delete {
        table: String,
        filter: Option<Box<PhysicalPlanNode>>,
    },

    /// CREATE TABLE execution
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
        constraints: Vec<TableConstraint>,
    },
}

/// Physical plan node for query execution trees
#[derive(Debug, Clone)]
pub enum PhysicalPlanNode {
    /// Sequential scan of a table
    SeqScan {
        table_name: String,
        alias: Option<String>,
    },

    /// Index scan of a table
    IndexScan {
        table_name: String,
        index_name: String,
        alias: Option<String>,
    },

    /// Filter rows based on predicate
    Filter {
        input: Box<PhysicalPlanNode>,
        predicate: LogicalExpression,
    },

    /// Project specific columns
    Projection {
        input: Box<PhysicalPlanNode>,
        expressions: Vec<(LogicalExpression, Option<String>)>,
    },

    /// Sort rows
    Sort {
        input: Box<PhysicalPlanNode>,
        sort_exprs: Vec<(LogicalExpression, bool)>, // (expr, is_ascending)
    },

    /// Limit rows
    Limit {
        input: Box<PhysicalPlanNode>,
        limit: Option<LogicalExpression>,
        offset: Option<LogicalExpression>,
    },

    /// Nested loop join
    NestedLoopJoin {
        left: Box<PhysicalPlanNode>,
        right: Box<PhysicalPlanNode>,
        join_type: JoinType,
        condition: Option<LogicalExpression>,
    },

    /// Hash join
    HashJoin {
        left: Box<PhysicalPlanNode>,
        right: Box<PhysicalPlanNode>,
        join_type: JoinType,
        left_keys: Vec<LogicalExpression>,
        right_keys: Vec<LogicalExpression>,
    },

    /// Hash aggregate
    HashAggregate {
        input: Box<PhysicalPlanNode>,
        group_exprs: Vec<LogicalExpression>,
        aggregate_exprs: Vec<AggregateExpression>,
    },

    /// Remove duplicates (can be implemented as HashAggregate)
    Distinct { input: Box<PhysicalPlanNode> },

    /// Literal values
    Values { rows: Vec<Vec<LogicalExpression>> },
}

impl PhysicalPlan {
    /// Get a human-readable explanation of this plan
    pub fn explain(&self) -> String {
        match self {
            PhysicalPlan::Select(node) => {
                let mut result = String::from("SELECT:\n");
                result.push_str(&node.explain(1));
                result
            }
            PhysicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                format!(
                    "INSERT INTO {} ({}) VALUES ({} rows)",
                    table,
                    columns
                        .as_ref()
                        .map(|c| c.join(", "))
                        .unwrap_or_else(|| "all columns".to_string()),
                    values.len()
                )
            }
            PhysicalPlan::CreateTable {
                table_name,
                columns,
                ..
            } => {
                format!("CREATE TABLE {} ({} columns)", table_name, columns.len())
            }
            PhysicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                let mut result = format!("UPDATE {} SET {} assignments", table, assignments.len());
                if filter.is_some() {
                    result.push_str(" WHERE ...");
                }
                result
            }
            PhysicalPlan::Delete { table, filter } => {
                let mut result = format!("DELETE FROM {}", table);
                if filter.is_some() {
                    result.push_str(" WHERE ...");
                }
                result
            }
        }
    }
}

impl PhysicalPlanNode {
    /// Get a human-readable explanation of this node
    pub fn explain(&self, indent_level: usize) -> String {
        let indent = "  ".repeat(indent_level);

        match self {
            PhysicalPlanNode::SeqScan { table_name, alias } => {
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                format!("{}SeqScan: {}{}", indent, table_name, alias_str)
            }

            PhysicalPlanNode::IndexScan {
                table_name,
                index_name,
                alias,
            } => {
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                format!(
                    "{}IndexScan: {} using {}{}",
                    indent, table_name, index_name, alias_str
                )
            }

            PhysicalPlanNode::Filter { input, predicate } => {
                let mut result = format!("{}Filter: {:?}\n", indent, predicate);
                result.push_str(&input.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::Projection { input, expressions } => {
                let proj_str = expressions
                    .iter()
                    .map(|(expr, alias)| {
                        if let Some(alias) = alias {
                            format!("{:?} AS {}", expr, alias)
                        } else {
                            format!("{:?}", expr)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut result = format!("{}Projection: {}\n", indent, proj_str);
                result.push_str(&input.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::Sort { input, sort_exprs } => {
                let sort_str = sort_exprs
                    .iter()
                    .map(|(expr, asc)| format!("{:?} {}", expr, if *asc { "ASC" } else { "DESC" }))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut result = format!("{}Sort: {}\n", indent, sort_str);
                result.push_str(&input.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::Limit {
                input,
                limit,
                offset,
            } => {
                let limit_str = limit
                    .as_ref()
                    .map(|l| format!("{:?}", l))
                    .unwrap_or_else(|| "NONE".to_string());
                let offset_str = offset
                    .as_ref()
                    .map(|o| format!(" OFFSET {:?}", o))
                    .unwrap_or_default();
                let mut result = format!("{}Limit: {}{}\n", indent, limit_str, offset_str);
                result.push_str(&input.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
            } => {
                let cond_str = condition
                    .as_ref()
                    .map(|c| format!(" ON {:?}", c))
                    .unwrap_or_default();
                let mut result = format!("{}NestedLoopJoin: {:?}{}\n", indent, join_type, cond_str);
                result.push_str(&left.explain(indent_level + 1));
                result.push('\n');
                result.push_str(&right.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
            } => {
                let mut result = format!("{}HashJoin: {:?}\n", indent, join_type);
                result.push_str(&format!("{}  Left keys: {:?}\n", indent, left_keys));
                result.push_str(&format!("{}  Right keys: {:?}\n", indent, right_keys));
                result.push_str(&left.explain(indent_level + 1));
                result.push('\n');
                result.push_str(&right.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::HashAggregate {
                input,
                group_exprs,
                aggregate_exprs,
            } => {
                let mut result = format!("{}HashAggregate:\n", indent);
                if !group_exprs.is_empty() {
                    result.push_str(&format!("{}  Group by: {:?}\n", indent, group_exprs));
                }
                if !aggregate_exprs.is_empty() {
                    result.push_str(&format!("{}  Aggregates: {:?}\n", indent, aggregate_exprs));
                }
                result.push_str(&input.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::Distinct { input } => {
                let mut result = format!("{}Distinct\n", indent);
                result.push_str(&input.explain(indent_level + 1));
                result
            }

            PhysicalPlanNode::Values { rows } => {
                format!("{}Values: {} rows", indent, rows.len())
            }
        }
    }

    /// Estimate the number of rows this node will produce
    pub fn estimated_row_count(&self) -> usize {
        // Very simple estimates for now
        // In a real system, this would use table statistics
        match self {
            PhysicalPlanNode::SeqScan { .. } => 1000, // Assume 1000 rows per table
            PhysicalPlanNode::IndexScan { .. } => 100, // Index scans are more selective
            PhysicalPlanNode::Filter { input, .. } => input.estimated_row_count() / 2, // 50% selectivity
            PhysicalPlanNode::Projection { input, .. } => input.estimated_row_count(),
            PhysicalPlanNode::Sort { input, .. } => input.estimated_row_count(),
            PhysicalPlanNode::Limit { .. } => {
                // If we can evaluate the limit statically, use that
                // Otherwise assume 100
                100
            }
            PhysicalPlanNode::NestedLoopJoin { left, right, .. } => {
                // Cartesian product for now
                left.estimated_row_count() * right.estimated_row_count()
            }
            PhysicalPlanNode::HashJoin { left, right, .. } => {
                // Assume 10% match rate
                (left.estimated_row_count() * right.estimated_row_count()) / 10
            }
            PhysicalPlanNode::HashAggregate {
                input, group_exprs, ..
            } => {
                if group_exprs.is_empty() {
                    1 // No GROUP BY means one result row
                } else {
                    input.estimated_row_count() / 10 // Assume 10 rows per group
                }
            }
            PhysicalPlanNode::Distinct { input } => input.estimated_row_count() / 2, // 50% distinct
            PhysicalPlanNode::Values { rows } => rows.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::Value;

    #[test]
    fn test_physical_plan_explain() {
        let plan = PhysicalPlan::Select(Box::new(PhysicalPlanNode::Projection {
            input: Box::new(PhysicalPlanNode::Filter {
                input: Box::new(PhysicalPlanNode::SeqScan {
                    table_name: "users".to_string(),
                    alias: None,
                }),
                predicate: LogicalExpression::BinaryOp {
                    left: Box::new(LogicalExpression::column("age")),
                    op: crate::planner::logical::BinaryOp::Gt,
                    right: Box::new(LogicalExpression::literal(Value::Int32(18))),
                },
            }),
            expressions: vec![(LogicalExpression::column("name"), None)],
        }));

        let explanation = plan.explain();
        assert!(explanation.contains("SELECT:"));
        assert!(explanation.contains("Projection:"));
        assert!(explanation.contains("Filter:"));
        assert!(explanation.contains("SeqScan: users"));
    }

    #[test]
    fn test_row_count_estimation() {
        let scan = PhysicalPlanNode::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        assert_eq!(scan.estimated_row_count(), 1000);

        let filter = PhysicalPlanNode::Filter {
            input: Box::new(scan),
            predicate: LogicalExpression::literal(Value::Boolean(true)),
        };
        assert_eq!(filter.estimated_row_count(), 500); // 50% of 1000

        let values = PhysicalPlanNode::Values {
            rows: vec![vec![], vec![], vec![]],
        };
        assert_eq!(values.estimated_row_count(), 3);
    }

    #[test]
    fn test_join_plan() {
        let left = Box::new(PhysicalPlanNode::SeqScan {
            table_name: "users".to_string(),
            alias: Some("u".to_string()),
        });

        let right = Box::new(PhysicalPlanNode::SeqScan {
            table_name: "orders".to_string(),
            alias: Some("o".to_string()),
        });

        let hash_join = PhysicalPlanNode::HashJoin {
            left: left.clone(),
            right: right.clone(),
            join_type: JoinType::Inner,
            left_keys: vec![LogicalExpression::column("id")],
            right_keys: vec![LogicalExpression::column("user_id")],
        };

        let nl_join = PhysicalPlanNode::NestedLoopJoin {
            left,
            right,
            join_type: JoinType::Inner,
            condition: Some(LogicalExpression::BinaryOp {
                left: Box::new(LogicalExpression::QualifiedColumn(
                    "u".to_string(),
                    "id".to_string(),
                )),
                op: crate::planner::logical::BinaryOp::Eq,
                right: Box::new(LogicalExpression::QualifiedColumn(
                    "o".to_string(),
                    "user_id".to_string(),
                )),
            }),
        };

        // Hash join should have better selectivity estimate
        assert!(hash_join.estimated_row_count() < nl_join.estimated_row_count());
    }
}
