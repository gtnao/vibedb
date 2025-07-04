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
    /// WAL manager.
    pub wal_manager: Arc<crate::storage::wal::manager::WalManager>,
    /// Transaction manager.
    pub transaction_manager: Arc<crate::transaction::manager::TransactionManager>,
    /// MVCC manager.
    pub mvcc_manager: Arc<crate::concurrency::mvcc::MVCCManager>,
    /// Session ID for tracking.
    pub session_id: u64,
    /// Current transaction ID (if any).
    pub current_transaction: Option<crate::transaction::id::TransactionId>,
}

impl Session {
    /// Creates a new session.
    pub fn new(database: Arc<Database>) -> Self {
        let catalog = database.catalog.clone();
        let buffer_pool = database.buffer_pool.clone();
        let wal_manager = database.wal_manager.clone();
        let transaction_manager = database.transaction_manager.clone();
        let mvcc_manager = database.mvcc_manager.clone();

        // Generate a simple session ID
        let session_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Session {
            database,
            catalog,
            buffer_pool,
            wal_manager,
            transaction_manager,
            mvcc_manager,
            session_id,
            current_transaction: None,
        }
    }

    /// Executes a SQL query and returns the results.
    pub fn execute_sql(&mut self, sql: &str) -> Result<QueryResult> {
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
                let context = ExecutionContext::with_managers(
                    self.catalog.clone(),
                    self.buffer_pool.clone(),
                    self.wal_manager.clone(),
                    self.transaction_manager.clone(),
                    self.mvcc_manager.clone(),
                    self.current_transaction,
                );
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

                let context = ExecutionContext::with_managers(
                    self.catalog.clone(),
                    self.buffer_pool.clone(),
                    self.wal_manager.clone(),
                    self.transaction_manager.clone(),
                    self.mvcc_manager.clone(),
                    self.current_transaction,
                );
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
                table_name,
                columns,
                constraints: _,
            } => {
                // Convert SQL DataType to access DataType
                let converted_columns: Vec<(&str, crate::access::DataType)> = columns
                    .iter()
                    .map(|col| {
                        let data_type = match &col.data_type {
                            crate::sql::ast::DataType::Int
                            | crate::sql::ast::DataType::SmallInt => crate::access::DataType::Int32,
                            crate::sql::ast::DataType::BigInt => crate::access::DataType::Int32, // TODO: Add Int64
                            crate::sql::ast::DataType::Boolean => crate::access::DataType::Boolean,
                            crate::sql::ast::DataType::Varchar(_)
                            | crate::sql::ast::DataType::Char(_)
                            | crate::sql::ast::DataType::Text => crate::access::DataType::Varchar,
                            _ => {
                                return Err(anyhow!(
                                    "Unsupported data type: {:?}",
                                    col.data_type
                                ))
                            }
                        };
                        Ok((col.name.as_str(), data_type))
                    })
                    .collect::<Result<Vec<_>>>()?;
                
                // Create the table using the catalog
                let _table_info = self.catalog.create_table_with_columns(
                    &table_name,
                    converted_columns.into_iter().collect(),
                )?;
                
                Ok(QueryResult::CreateTable { 
                    table_name: table_name.clone() 
                })
            }
            PhysicalPlan::Delete { table, filter } => {
                let context = ExecutionContext::with_managers(
                    self.catalog.clone(),
                    self.buffer_pool.clone(),
                    self.wal_manager.clone(),
                    self.transaction_manager.clone(),
                    self.mvcc_manager.clone(),
                    self.current_transaction,
                );

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
                let context = ExecutionContext::with_managers(
                    self.catalog.clone(),
                    self.buffer_pool.clone(),
                    self.wal_manager.clone(),
                    self.transaction_manager.clone(),
                    self.mvcc_manager.clone(),
                    self.current_transaction,
                );

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
            PhysicalPlan::BeginTransaction => {
                // Check if there's already an active transaction
                if self.current_transaction.is_some() {
                    return Err(anyhow!("A transaction is already in progress"));
                }
                
                // Begin a new transaction
                let tx_id = self.transaction_manager.begin()?;
                self.current_transaction.replace(tx_id);
                Ok(QueryResult::Transaction {
                    message: format!("BEGIN (Transaction ID: {:?})", tx_id),
                })
            }
            PhysicalPlan::Commit => {
                // Commit the current transaction
                if let Some(tx_id) = self.current_transaction {
                    self.transaction_manager.commit(tx_id)?;
                    self.current_transaction.take();
                    Ok(QueryResult::Transaction {
                        message: "COMMIT".to_string(),
                    })
                } else {
                    Err(anyhow!("No active transaction"))
                }
            }
            PhysicalPlan::Rollback => {
                // Rollback the current transaction
                if let Some(tx_id) = self.current_transaction {
                    self.transaction_manager.abort(tx_id)?;
                    self.current_transaction.take();
                    Ok(QueryResult::Transaction {
                        message: "ROLLBACK".to_string(),
                    })
                } else {
                    Err(anyhow!("No active transaction"))
                }
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
            PhysicalPlanNode::HashAggregate {
                input,
                group_exprs,
                aggregate_exprs,
            } => {
                let child = self.create_executor(*input, context)?;

                // Convert group by expressions
                let group_indices: Vec<usize> = group_exprs
                    .into_iter()
                    .filter_map(|expr| {
                        // For now, only support column references in GROUP BY
                        match expr {
                            LogicalExpression::Column(name) => {
                                // This is a simplified approach - in a real system we'd resolve column names to indices
                                // For now, assume it's a column index encoded as a string
                                name.parse::<usize>().ok()
                            }
                            _ => None,
                        }
                    })
                    .collect();

                // Convert aggregate expressions
                let agg_specs: Vec<crate::executor::AggregateSpec> = aggregate_exprs
                    .into_iter()
                    .map(|agg_expr| {
                        let func = match agg_expr.function.to_uppercase().as_str() {
                            "COUNT" => crate::executor::AggregateFunction::Count,
                            "SUM" => crate::executor::AggregateFunction::Sum,
                            "AVG" => crate::executor::AggregateFunction::Avg,
                            "MIN" => crate::executor::AggregateFunction::Min,
                            "MAX" => crate::executor::AggregateFunction::Max,
                            _ => crate::executor::AggregateFunction::Count, // Default
                        };

                        // Extract column index from first argument
                        let column_idx = if agg_expr.args.is_empty() {
                            None // COUNT(*)
                        } else {
                            match &agg_expr.args[0] {
                                LogicalExpression::Column(name) => name.parse::<usize>().ok(),
                                _ => None,
                            }
                        };

                        let mut spec = crate::executor::AggregateSpec::new(func, column_idx);
                        if let Some(alias) = agg_expr.alias {
                            spec.alias = Some(alias);
                        }
                        spec
                    })
                    .collect();

                Ok(Box::new(crate::executor::HashAggregateExecutor::new(
                    child,
                    crate::executor::GroupByClause::new(group_indices),
                    agg_specs,
                )?))
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
    /// Result of a transaction control command.
    Transaction { message: String },
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
        assert!(session.current_transaction.is_none());

        // Clean up
        std::fs::remove_file("test_session.db").ok();

        Ok(())
    }

    #[test]
    fn test_transaction_lifecycle() -> Result<()> {
        let db = Arc::new(Database::create(Path::new("test_session_tx.db"))?);
        let mut session = Session::new(db.clone());

        // Begin transaction
        let result = session.execute_sql("BEGIN")?;
        assert!(matches!(result, QueryResult::Transaction { .. }));
        assert!(session.current_transaction.is_some());

        // Cannot begin another transaction
        let result = session.execute_sql("BEGIN");
        assert!(result.is_err());

        // Commit transaction
        let result = session.execute_sql("COMMIT")?;
        assert!(matches!(result, QueryResult::Transaction { .. }));
        assert!(session.current_transaction.is_none());

        // Cannot commit without transaction
        let result = session.execute_sql("COMMIT");
        assert!(result.is_err());

        // Begin and rollback
        session.execute_sql("BEGIN")?;
        session.execute_sql("ROLLBACK")?;
        assert!(session.current_transaction.is_none());

        // Clean up
        std::fs::remove_file("test_session_tx.db").ok();

        Ok(())
    }
}
