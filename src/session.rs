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

                let mut count = 0;
                while executor.next()?.is_some() {
                    count += 1;
                }

                Ok(QueryResult::Insert {
                    table_name: table,
                    row_count: count,
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
                let child = self.create_executor(*input, context.clone())?;

                // Convert LogicalExpression to Expression
                let expr = self.logical_to_expression(predicate)?;

                Ok(Box::new(crate::executor::FilterExecutor::new(child, expr)))
            }
            PhysicalPlanNode::Projection { input, expressions } => {
                let child = self.create_executor(*input, context)?;

                // For now, we'll just project all columns
                // TODO: Implement proper projection handling
                let column_indices: Vec<usize> = (0..expressions.len()).collect();

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
        use crate::expression::{BinaryOperator, Expression};
        use crate::planner::logical::BinaryOp;

        match expr {
            LogicalExpression::Literal(val) => Ok(Expression::literal(val)),
            LogicalExpression::Column(_name) => {
                // For now, we'll use column index 0
                // TODO: Resolve column name to index
                Ok(Expression::column(0))
            }
            LogicalExpression::BinaryOp { left, op, right } => {
                let left_expr = self.logical_to_expression(*left)?;
                let right_expr = self.logical_to_expression(*right)?;

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
