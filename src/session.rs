//! Session management for database connections.

use crate::access::Value;
use crate::catalog::Catalog;
use crate::database::Database;
use crate::executor::{ExecutionContext, Executor};
use crate::planner::logical::LogicalExpression;
use crate::planner::{PhysicalPlan, PhysicalPlanNode, Planner};
use crate::sql::parser::Parser;
use crate::storage::buffer::BufferPoolManager;
use anyhow::{anyhow, Result};
use std::sync::Arc;

/// Represents a database session for a connected client.
pub struct Session {
    /// The database instance.
    pub database: Arc<Database>,
    /// The catalog (schema).
    pub catalog: Arc<Catalog>,
    /// The buffer pool manager.
    pub buffer_pool: Arc<BufferPoolManager>,
    /// Session ID for tracking.
    pub session_id: u64,
    /// Current transaction ID (if any).
    pub transaction_id: Option<u64>,
}

impl Session {
    /// Creates a new session.
    pub fn new(database: Arc<Database>) -> Self {
        let catalog = database.catalog.clone();
        let buffer_pool = database.buffer_pool.clone();

        // Generate a simple session ID
        let session_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Session {
            database,
            catalog,
            buffer_pool,
            session_id,
            transaction_id: None,
        }
    }

    /// Executes a SQL query and returns the results.
    pub fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        eprintln!("DEBUG: Executing SQL: {}", sql);

        // Parse the SQL
        let mut parser = Parser::new(sql.to_string());
        let statement = parser.parse()?;
        eprintln!("DEBUG: Parsed statement successfully");

        // Plan the query
        let planner = Planner::new(self.catalog.clone());
        let physical_plan = planner.plan(statement)?;
        eprintln!("DEBUG: Created physical plan");

        // Execute the plan
        match physical_plan {
            PhysicalPlan::Select(plan_node) => {
                eprintln!("DEBUG: Executing SELECT plan");
                let context = ExecutionContext::new(self.catalog.clone(), self.buffer_pool.clone());
                let mut executor = self.create_executor(*plan_node, context)?;
                eprintln!("DEBUG: Created executor");

                executor.init()?;
                eprintln!("DEBUG: Initialized executor");
                let schema = executor.output_schema().to_vec();
                eprintln!("DEBUG: Got schema with {} columns", schema.len());

                let mut rows = Vec::new();
                while let Some(tuple) = executor.next()? {
                    rows.push(tuple);
                }
                eprintln!("DEBUG: Collected {} rows", rows.len());

                Ok(QueryResult::Select { schema, rows })
            }
            PhysicalPlan::Insert {
                table,
                columns: _,
                values,
            } => {
                // Convert LogicalExpression values to Value
                let value_rows = values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| match expr {
                                LogicalExpression::Literal(val) => Ok(val),
                                _ => Err(anyhow!("Only literal values supported in INSERT")),
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;

                let context = ExecutionContext::new(self.catalog.clone(), self.buffer_pool.clone());
                let mut executor =
                    crate::executor::InsertExecutor::new(table.clone(), value_rows, context);
                executor.init()?;

                let mut actual_count = 0;

                // InsertExecutor returns a single tuple containing the count
                if let Some(tuple) = executor.next()? {
                    // Deserialize the count from the tuple
                    let count_schema = vec![crate::access::DataType::Int32];
                    let values = crate::access::deserialize_values(&tuple.data, &count_schema)?;
                    if let crate::access::Value::Int32(count) = &values[0] {
                        actual_count = *count as usize;
                    }
                }

                Ok(QueryResult::Insert {
                    table_name: table,
                    row_count: actual_count,
                })
            }
            PhysicalPlan::CreateTable {
                table_name: _,
                columns: _,
                constraints: _,
            } => {
                // CREATE TABLE is not supported through SQL yet
                // Tables must be created programmatically
                Err(anyhow!("CREATE TABLE not supported through SQL. Tables must be created programmatically."))
            }
            PhysicalPlan::Delete { table, filter } => {
                let context = ExecutionContext::new(self.catalog.clone(), self.buffer_pool.clone());

                // Create child executor if there's a filter
                let child = if let Some(filter_node) = filter {
                    Some(self.create_executor(*filter_node, context.clone())?)
                } else {
                    None
                };

                let mut executor =
                    crate::executor::DeleteExecutor::new(table.clone(), child, context);
                executor.init()?;

                let mut actual_count = 0;

                // DeleteExecutor returns a single tuple containing the count
                if let Some(tuple) = executor.next()? {
                    // Deserialize the count from the tuple
                    let count_schema = vec![crate::access::DataType::Int32];
                    let values = crate::access::deserialize_values(&tuple.data, &count_schema)?;
                    if let crate::access::Value::Int32(count) = &values[0] {
                        actual_count = *count as usize;
                    }
                }

                Ok(QueryResult::Delete {
                    table_name: table,
                    row_count: actual_count,
                })
            }
            PhysicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                let context = ExecutionContext::new(self.catalog.clone(), self.buffer_pool.clone());

                // Convert assignments to UpdateExpressions
                let update_expressions: Vec<crate::executor::UpdateExpression> = assignments
                    .into_iter()
                    .map(|(column, expr)| {
                        // For now, only support literal values in UPDATE
                        match expr {
                            LogicalExpression::Literal(val) => {
                                Ok(crate::executor::UpdateExpression::new(column, val))
                            }
                            _ => Err(anyhow!(
                                "Only literal values supported in UPDATE SET clause"
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Create child executor if there's a filter
                let child = if let Some(filter_node) = filter {
                    Some(self.create_executor(*filter_node, context.clone())?)
                } else {
                    None
                };

                let mut executor = crate::executor::UpdateExecutor::new(
                    table.clone(),
                    update_expressions,
                    child,
                    context,
                );
                executor.init()?;

                let mut actual_count = 0;

                // UpdateExecutor returns a single tuple containing the count
                if let Some(tuple) = executor.next()? {
                    // Deserialize the count from the tuple
                    let count_schema = vec![crate::access::DataType::Int32];
                    let values = crate::access::deserialize_values(&tuple.data, &count_schema)?;
                    if let crate::access::Value::Int32(count) = &values[0] {
                        actual_count = *count as usize;
                    }
                }

                Ok(QueryResult::Update {
                    table_name: table,
                    row_count: actual_count,
                })
            }
        }
    }

    /// Creates an executor from a physical plan node.
    fn create_executor(
        &self,
        plan_node: PhysicalPlanNode,
        context: ExecutionContext,
    ) -> Result<Box<dyn Executor>> {
        match plan_node {
            PhysicalPlanNode::SeqScan { table_name, .. } => Ok(Box::new(
                crate::executor::SeqScanExecutor::new(table_name, context),
            )),
            PhysicalPlanNode::Filter { input, predicate } => {
                let mut child = self.create_executor(*input, context.clone())?;

                // Initialize child first to get proper schema
                eprintln!("DEBUG: Initializing child executor for filter");
                child.init()?;

                let child_schema = child.output_schema();

                eprintln!(
                    "DEBUG: Filter child schema has {} columns",
                    child_schema.len()
                );
                for (i, col) in child_schema.iter().enumerate() {
                    eprintln!(
                        "DEBUG: Column[{}]: name='{}', type={:?}",
                        i, col.name, col.data_type
                    );
                }

                // Convert LogicalExpression to Expression with schema context
                eprintln!("DEBUG: Converting predicate expression with schema");
                let expr = self.logical_to_expression_with_schema(predicate, child_schema)?;
                eprintln!("DEBUG: Converted predicate expression successfully");

                Ok(Box::new(crate::executor::FilterExecutor::new(child, expr)))
            }
            PhysicalPlanNode::Projection { input, expressions } => {
                let mut child = self.create_executor(*input, context)?;

                // Initialize child first to get proper schema
                eprintln!("DEBUG: Initializing child executor for projection");
                child.init()?;

                let child_schema = child.output_schema();

                eprintln!("DEBUG: Child schema has {} columns", child_schema.len());
                for (i, col) in child_schema.iter().enumerate() {
                    eprintln!(
                        "DEBUG: Column[{}]: name='{}', type={:?}",
                        i, col.name, col.data_type
                    );
                }

                // Resolve column indices from expressions
                let mut column_indices = Vec::new();
                for (expr, _alias) in &expressions {
                    eprintln!("DEBUG: Processing projection expression: {:?}", expr);
                    match expr {
                        LogicalExpression::Column(name) => {
                            // Find column index by name
                            let index = child_schema
                                .iter()
                                .position(|col| col.name == *name)
                                .ok_or_else(|| {
                                    eprintln!("ERROR: Column '{}' not found in schema", name);
                                    anyhow!("Column '{}' not found", name)
                                })?;
                            eprintln!("DEBUG: Resolved column '{}' to index {}", name, index);
                            column_indices.push(index);
                        }
                        _ => {
                            // For now, only support column references in projection
                            return Err(anyhow!("Only column references supported in projection"));
                        }
                    }
                }

                Ok(Box::new(crate::executor::ProjectionExecutor::new(
                    child,
                    column_indices,
                )))
            }
            PhysicalPlanNode::Sort {
                input,
                sort_exprs: _,
            } => {
                let child = self.create_executor(*input, context)?;

                // For now, we'll use a simple sort
                // TODO: Implement proper sort handling
                Ok(Box::new(crate::executor::SortExecutor::new(child, vec![])))
            }
            PhysicalPlanNode::Limit {
                input,
                limit,
                offset,
            } => {
                let child = self.create_executor(*input, context)?;

                let limit_count = match limit {
                    Some(LogicalExpression::Literal(Value::Int32(n))) => n as usize,
                    _ => usize::MAX, // No limit means unlimited
                };

                let offset_count = match offset {
                    Some(LogicalExpression::Literal(Value::Int32(n))) => n as usize,
                    _ => 0,
                };

                Ok(Box::new(crate::executor::LimitExecutor::with_offset(
                    child,
                    limit_count,
                    offset_count,
                )))
            }
            _ => Err(anyhow!("Unsupported physical plan node")),
        }
    }

    /// Converts a LogicalExpression to an Expression for the executor.
    fn logical_to_expression(
        &self,
        expr: LogicalExpression,
    ) -> Result<crate::expression::Expression> {
        self.logical_to_expression_with_schema(expr, &[])
    }

    /// Converts a LogicalExpression to an Expression for the executor with schema context.
    fn logical_to_expression_with_schema(
        &self,
        expr: LogicalExpression,
        schema: &[crate::executor::ColumnInfo],
    ) -> Result<crate::expression::Expression> {
        use crate::expression::{BinaryOperator, Expression};
        use crate::planner::logical::BinaryOp;

        match expr {
            LogicalExpression::Literal(val) => Ok(Expression::literal(val)),
            LogicalExpression::Column(name) => {
                if schema.is_empty() {
                    // No schema provided, use column index 0 as fallback
                    // This happens during planning phase
                    eprintln!(
                        "WARNING: No schema provided for column '{}', using index 0",
                        name
                    );
                    Ok(Expression::column(0))
                } else {
                    // Find column index by name
                    eprintln!("DEBUG: Resolving column '{}' in schema", name);
                    let index =
                        schema
                            .iter()
                            .position(|col| col.name == name)
                            .ok_or_else(|| {
                                eprintln!("ERROR: Column '{}' not found in schema", name);
                                anyhow!("Column '{}' not found", name)
                            })?;
                    eprintln!("DEBUG: Column '{}' resolved to index {}", name, index);
                    Ok(Expression::column(index))
                }
            }
            LogicalExpression::BinaryOp { left, op, right } => {
                let left_expr = self.logical_to_expression_with_schema(*left, schema)?;
                let right_expr = self.logical_to_expression_with_schema(*right, schema)?;

                let binary_op = match op {
                    BinaryOp::Eq => BinaryOperator::Eq,
                    BinaryOp::NotEq => BinaryOperator::Ne,
                    BinaryOp::Lt => BinaryOperator::Lt,
                    BinaryOp::LtEq => BinaryOperator::Le,
                    BinaryOp::Gt => BinaryOperator::Gt,
                    BinaryOp::GtEq => BinaryOperator::Ge,
                    BinaryOp::And => BinaryOperator::And,
                    BinaryOp::Or => BinaryOperator::Or,
                    BinaryOp::Add => BinaryOperator::Add,
                    BinaryOp::Subtract => BinaryOperator::Sub,
                    BinaryOp::Multiply => BinaryOperator::Mul,
                    BinaryOp::Divide => BinaryOperator::Div,
                    BinaryOp::Modulo => return Err(anyhow!("Modulo operator not supported")),
                };

                Ok(Expression::binary_op(binary_op, left_expr, right_expr))
            }
            _ => Err(anyhow!("Unsupported expression type")),
        }
    }

    /// Begins a new transaction.
    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.transaction_id.is_some() {
            return Err(anyhow!("Transaction already in progress"));
        }
        // TODO: Implement actual transaction begin
        self.transaction_id = Some(1);
        Ok(())
    }

    /// Commits the current transaction.
    pub fn commit_transaction(&mut self) -> Result<()> {
        if self.transaction_id.is_none() {
            return Err(anyhow!("No transaction in progress"));
        }
        // TODO: Implement actual transaction commit
        self.transaction_id = None;
        Ok(())
    }

    /// Rolls back the current transaction.
    pub fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction_id.is_none() {
            return Err(anyhow!("No transaction in progress"));
        }
        // TODO: Implement actual transaction rollback
        self.transaction_id = None;
        Ok(())
    }
}

/// Result of executing a query.
pub enum QueryResult {
    /// Result of a SELECT query.
    Select {
        schema: Vec<crate::executor::ColumnInfo>,
        rows: Vec<crate::access::Tuple>,
    },
    /// Result of an INSERT query.
    Insert {
        table_name: String,
        row_count: usize,
    },
    /// Result of an UPDATE query.
    Update {
        table_name: String,
        row_count: usize,
    },
    /// Result of a DELETE query.
    Delete {
        table_name: String,
        row_count: usize,
    },
    /// Result of a CREATE TABLE query.
    CreateTable { table_name: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_session_creation() -> Result<()> {
        let db = Arc::new(Database::create(Path::new("test_session.db"))?);
        let session = Session::new(db.clone());

        assert!(session.session_id > 0);
        assert!(session.transaction_id.is_none());

        // Clean up
        std::fs::remove_file("test_session.db").ok();

        Ok(())
    }

    #[test]
    fn test_transaction_lifecycle() -> Result<()> {
        let db = Arc::new(Database::create(Path::new("test_session_tx.db"))?);
        let mut session = Session::new(db.clone());

        // Begin transaction
        session.begin_transaction()?;
        assert!(session.transaction_id.is_some());

        // Cannot begin another transaction
        assert!(session.begin_transaction().is_err());

        // Commit transaction
        session.commit_transaction()?;
        assert!(session.transaction_id.is_none());

        // Cannot commit without transaction
        assert!(session.commit_transaction().is_err());

        // Begin and rollback
        session.begin_transaction()?;
        session.rollback_transaction()?;
        assert!(session.transaction_id.is_none());

        // Clean up
        std::fs::remove_file("test_session_tx.db").ok();

        Ok(())
    }
}
