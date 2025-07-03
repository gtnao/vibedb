//! Query planner for converting SQL AST to executable plans.
//!
//! The planner performs the following transformations:
//! 1. SQL AST -> Logical Plan: High-level, optimizer-friendly representation
//! 2. Logical Plan -> Physical Plan: Concrete executor plan with algorithms chosen
//!
//! Currently implements simple rule-based planning with minimal optimization.

pub mod logical;
pub mod physical;

use crate::catalog::Catalog;
use crate::sql::ast::{
    CreateTableStatement, DeleteStatement, InsertStatement, SelectStatement, Statement,
    UpdateStatement,
};
use anyhow::{anyhow, Result};
use std::sync::Arc;

pub use logical::{LogicalExpression, LogicalPlan, LogicalPlanNode};
pub use physical::{PhysicalPlan, PhysicalPlanNode};

/// Query planner that converts SQL AST to executable plans
pub struct Planner {
    #[allow(dead_code)] // Will be used when catalog validation is enabled
    catalog: Arc<Catalog>,
}

impl Planner {
    /// Create a new planner with the given catalog
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    /// Convert SQL statement to logical plan
    pub fn statement_to_logical(&self, stmt: Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Select(select) => self.select_to_logical(select),
            Statement::Insert(insert) => self.insert_to_logical(insert),
            Statement::CreateTable(create) => self.create_table_to_logical(create),
            Statement::Update(update) => self.update_to_logical(update),
            Statement::Delete(delete) => self.delete_to_logical(delete),
            Statement::DropTable(_) => Err(anyhow!("DROP TABLE not yet implemented in planner")),
            Statement::CreateIndex(_) => {
                Err(anyhow!("CREATE INDEX not yet implemented in planner"))
            }
            Statement::DropIndex(_) => Err(anyhow!("DROP INDEX not yet implemented in planner")),
        }
    }

    /// Convert logical plan to physical plan
    pub fn logical_to_physical(&self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        match logical {
            LogicalPlan::Select(root) => {
                let physical_root = self.logical_node_to_physical(root)?;
                Ok(PhysicalPlan::Select(physical_root))
            }
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => Ok(PhysicalPlan::Insert {
                table,
                columns,
                values,
            }),
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => Ok(PhysicalPlan::Update {
                table,
                assignments,
                filter: filter
                    .map(|f| self.logical_node_to_physical(f))
                    .transpose()?,
            }),
            LogicalPlan::Delete { table, filter } => Ok(PhysicalPlan::Delete {
                table,
                filter: filter
                    .map(|f| self.logical_node_to_physical(f))
                    .transpose()?,
            }),
            LogicalPlan::CreateTable {
                table_name,
                columns,
                constraints,
            } => Ok(PhysicalPlan::CreateTable {
                table_name,
                columns,
                constraints,
            }),
        }
    }

    /// Convert SQL AST to physical plan (convenience method)
    pub fn plan(&self, stmt: Statement) -> Result<PhysicalPlan> {
        let logical = self.statement_to_logical(stmt)?;
        self.logical_to_physical(logical)
    }

    // Private helper methods

    fn select_to_logical(&self, select: SelectStatement) -> Result<LogicalPlan> {
        // Start with the FROM clause and track table name for * expansion
        let (mut plan, table_name) = if let Some(table_ref) = select.from {
            let table_name = table_ref.name.clone();
            (
                LogicalPlanNode::TableScan {
                    table_name: table_ref.name.clone(),
                    alias: table_ref.alias,
                },
                Some(table_name),
            )
        } else {
            // SELECT without FROM (e.g., SELECT 1)
            (LogicalPlanNode::Values { rows: vec![vec![]] }, None)
        };

        // Add JOINs
        for join in select.joins {
            plan = LogicalPlanNode::Join {
                left: Box::new(plan),
                right: Box::new(LogicalPlanNode::TableScan {
                    table_name: join.table.name,
                    alias: join.table.alias,
                }),
                join_type: self.convert_join_type(join.join_type),
                condition: join.on.map(|expr| self.convert_expression(expr)),
            };
        }

        // Add WHERE clause
        if let Some(where_expr) = select.where_clause {
            plan = LogicalPlanNode::Filter {
                input: Box::new(plan),
                predicate: self.convert_expression(where_expr),
            };
        }

        // Add GROUP BY
        if !select.group_by.is_empty() {
            let group_exprs: Vec<_> = select
                .group_by
                .into_iter()
                .map(|expr| self.convert_expression(expr))
                .collect();

            plan = LogicalPlanNode::GroupBy {
                input: Box::new(plan),
                group_exprs,
                aggregate_exprs: vec![], // Will be filled by projection analysis
            };
        }

        // Add HAVING
        if let Some(having_expr) = select.having {
            plan = LogicalPlanNode::Having {
                input: Box::new(plan),
                predicate: self.convert_expression(having_expr),
            };
        }

        // Add projection
        let projections = self.convert_select_items(select.projections, table_name.as_deref())?;
        plan = LogicalPlanNode::Projection {
            input: Box::new(plan),
            expressions: projections,
        };

        // Add DISTINCT
        if select.distinct {
            plan = LogicalPlanNode::Distinct {
                input: Box::new(plan),
            };
        }

        // Add ORDER BY
        if !select.order_by.is_empty() {
            let sort_exprs: Vec<_> = select
                .order_by
                .into_iter()
                .map(|item| {
                    (
                        self.convert_expression(item.expression),
                        item.direction == crate::sql::ast::OrderDirection::Asc,
                    )
                })
                .collect();

            plan = LogicalPlanNode::Sort {
                input: Box::new(plan),
                exprs: sort_exprs,
            };
        }

        // Add LIMIT/OFFSET
        if select.limit.is_some() || select.offset.is_some() {
            plan = LogicalPlanNode::Limit {
                input: Box::new(plan),
                limit: select.limit.map(|expr| self.convert_expression(expr)),
                offset: select.offset.map(|expr| self.convert_expression(expr)),
            };
        }

        Ok(LogicalPlan::Select(Box::new(plan)))
    }

    fn insert_to_logical(&self, insert: InsertStatement) -> Result<LogicalPlan> {
        // TODO: Validate table exists once catalog supports mutable operations
        // self.catalog.get_table(&insert.table_name)?;

        // Convert expressions
        let value_rows: Vec<Vec<LogicalExpression>> = insert
            .values
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| self.convert_expression(expr))
                    .collect()
            })
            .collect();

        Ok(LogicalPlan::Insert {
            table: insert.table_name,
            columns: insert.columns,
            values: value_rows,
        })
    }

    fn create_table_to_logical(&self, create: CreateTableStatement) -> Result<LogicalPlan> {
        Ok(LogicalPlan::CreateTable {
            table_name: create.table_name,
            columns: create.columns,
            constraints: create.constraints,
        })
    }

    fn delete_to_logical(&self, delete: DeleteStatement) -> Result<LogicalPlan> {
        // TODO: Validate table exists once catalog supports mutable operations
        // self.catalog.get_table(&delete.table_name)?;

        // Create filter node if WHERE clause exists
        let filter = if let Some(where_clause) = delete.where_clause {
            let table_scan = LogicalPlanNode::TableScan {
                table_name: delete.table_name.clone(),
                alias: None,
            };
            let predicate = self.convert_expression(where_clause);
            Some(Box::new(LogicalPlanNode::Filter {
                input: Box::new(table_scan),
                predicate,
            }))
        } else {
            None
        };

        Ok(LogicalPlan::Delete {
            table: delete.table_name,
            filter,
        })
    }

    fn update_to_logical(&self, update: UpdateStatement) -> Result<LogicalPlan> {
        // TODO: Validate table exists once catalog supports mutable operations
        // self.catalog.get_table(&update.table_name)?;

        // Convert assignments
        let assignments: Vec<(String, LogicalExpression)> = update
            .assignments
            .into_iter()
            .map(|assignment| {
                let expr = self.convert_expression(assignment.value);
                (assignment.column, expr)
            })
            .collect();

        // Create filter node if WHERE clause exists
        let filter = if let Some(where_clause) = update.where_clause {
            let table_scan = LogicalPlanNode::TableScan {
                table_name: update.table_name.clone(),
                alias: None,
            };
            let predicate = self.convert_expression(where_clause);
            Some(Box::new(LogicalPlanNode::Filter {
                input: Box::new(table_scan),
                predicate,
            }))
        } else {
            None
        };

        Ok(LogicalPlan::Update {
            table: update.table_name,
            assignments,
            filter,
        })
    }

    fn convert_expression(&self, expr: crate::sql::ast::Expression) -> LogicalExpression {
        use crate::sql::ast::Expression as SqlExpr;

        match expr {
            SqlExpr::Literal(value) => LogicalExpression::Literal(value),
            SqlExpr::Null => LogicalExpression::Null,
            SqlExpr::Column(name) => LogicalExpression::Column(name),
            SqlExpr::QualifiedColumn(table, column) => {
                LogicalExpression::QualifiedColumn(table, column)
            }
            SqlExpr::BinaryOp { left, op, right } => LogicalExpression::BinaryOp {
                left: Box::new(self.convert_expression(*left)),
                op: self.convert_binary_op(op),
                right: Box::new(self.convert_expression(*right)),
            },
            SqlExpr::UnaryOp { op, operand } => LogicalExpression::UnaryOp {
                op: self.convert_unary_op(op),
                operand: Box::new(self.convert_expression(*operand)),
            },
            SqlExpr::Function {
                name,
                args,
                distinct,
            } => LogicalExpression::Function {
                name,
                args: args
                    .into_iter()
                    .map(|e| self.convert_expression(e))
                    .collect(),
                distinct,
            },
            SqlExpr::Cast {
                expression,
                data_type,
            } => LogicalExpression::Cast {
                expression: Box::new(self.convert_expression(*expression)),
                data_type,
            },
            SqlExpr::IsNull {
                expression,
                negated,
            } => LogicalExpression::IsNull {
                expression: Box::new(self.convert_expression(*expression)),
                negated,
            },
            SqlExpr::InList {
                expression,
                list,
                negated,
            } => LogicalExpression::InList {
                expression: Box::new(self.convert_expression(*expression)),
                list: list
                    .into_iter()
                    .map(|e| self.convert_expression(e))
                    .collect(),
                negated,
            },
            SqlExpr::Between {
                expression,
                low,
                high,
                negated,
            } => LogicalExpression::Between {
                expression: Box::new(self.convert_expression(*expression)),
                low: Box::new(self.convert_expression(*low)),
                high: Box::new(self.convert_expression(*high)),
                negated,
            },
            SqlExpr::Like {
                expression,
                pattern,
                escape,
                negated,
            } => LogicalExpression::Like {
                expression: Box::new(self.convert_expression(*expression)),
                pattern: Box::new(self.convert_expression(*pattern)),
                escape: escape.map(|e| Box::new(self.convert_expression(*e))),
                negated,
            },
            // TODO: Implement CASE, EXISTS, Subquery
            _ => LogicalExpression::Null, // Placeholder
        }
    }

    fn convert_binary_op(&self, op: crate::sql::ast::BinaryOperator) -> logical::BinaryOp {
        use crate::sql::ast::BinaryOperator as SqlOp;
        use logical::BinaryOp;

        match op {
            SqlOp::Plus => BinaryOp::Add,
            SqlOp::Minus => BinaryOp::Subtract,
            SqlOp::Multiply => BinaryOp::Multiply,
            SqlOp::Divide => BinaryOp::Divide,
            SqlOp::Modulo => BinaryOp::Modulo,
            SqlOp::Equal => BinaryOp::Eq,
            SqlOp::NotEqual => BinaryOp::NotEq,
            SqlOp::Less => BinaryOp::Lt,
            SqlOp::Greater => BinaryOp::Gt,
            SqlOp::LessEqual => BinaryOp::LtEq,
            SqlOp::GreaterEqual => BinaryOp::GtEq,
            SqlOp::And => BinaryOp::And,
            SqlOp::Or => BinaryOp::Or,
        }
    }

    fn convert_unary_op(&self, op: crate::sql::ast::UnaryOperator) -> logical::UnaryOp {
        use crate::sql::ast::UnaryOperator as SqlOp;
        use logical::UnaryOp;

        match op {
            SqlOp::Not => UnaryOp::Not,
            SqlOp::Minus => UnaryOp::Minus,
            SqlOp::Plus => UnaryOp::Plus,
        }
    }

    fn convert_join_type(&self, join_type: crate::sql::ast::JoinType) -> logical::JoinType {
        use crate::sql::ast::JoinType as SqlJoin;
        use logical::JoinType;

        match join_type {
            SqlJoin::Inner => JoinType::Inner,
            SqlJoin::Left => JoinType::Left,
            SqlJoin::Right => JoinType::Right,
            SqlJoin::Full => JoinType::Full,
            SqlJoin::Cross => JoinType::Cross,
        }
    }

    fn convert_select_items(
        &self,
        items: Vec<crate::sql::ast::SelectItem>,
        table_name: Option<&str>,
    ) -> Result<Vec<(LogicalExpression, Option<String>)>> {
        use crate::sql::ast::SelectItem;

        let mut projections = Vec::new();

        for item in items {
            match item {
                SelectItem::AllColumns => {
                    // Expand * to all columns from the table
                    if let Some(table_name) = table_name {
                        // Get table info from catalog
                        let table_info = self
                            .catalog
                            .get_table(table_name)?
                            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;

                        // Add each column as a projection
                        if let Some(column_names) = &table_info.column_names {
                            for column_name in column_names {
                                projections
                                    .push((LogicalExpression::Column(column_name.clone()), None));
                            }
                        } else {
                            // Table exists but has no column information - return error for now
                            // In the future, we could load from pg_attribute
                            return Err(anyhow!(
                                "Table '{}' has no column information",
                                table_name
                            ));
                        }
                    } else {
                        return Err(anyhow!("SELECT * requires a FROM clause"));
                    }
                }
                SelectItem::AllColumnsFrom(table) => {
                    // Expand table.* to all columns from the specified table
                    let table_info = self
                        .catalog
                        .get_table(&table)?
                        .ok_or_else(|| anyhow!("Table '{}' not found", table))?;

                    if let Some(column_names) = &table_info.column_names {
                        for column_name in column_names {
                            projections.push((
                                LogicalExpression::QualifiedColumn(
                                    table.clone(),
                                    column_name.clone(),
                                ),
                                None,
                            ));
                        }
                    } else {
                        return Err(anyhow!("Table '{}' has no column information", table));
                    }
                }
                SelectItem::Expression(expr, alias) => {
                    projections.push((self.convert_expression(expr), alias));
                }
            }
        }

        Ok(projections)
    }

    fn logical_node_to_physical(
        &self,
        node: Box<LogicalPlanNode>,
    ) -> Result<Box<PhysicalPlanNode>> {
        match *node {
            LogicalPlanNode::TableScan { table_name, alias } => {
                Ok(Box::new(PhysicalPlanNode::SeqScan { table_name, alias }))
            }
            LogicalPlanNode::Filter { input, predicate } => {
                let child = self.logical_node_to_physical(input)?;
                Ok(Box::new(PhysicalPlanNode::Filter {
                    input: child,
                    predicate,
                }))
            }
            LogicalPlanNode::Projection { input, expressions } => {
                let child = self.logical_node_to_physical(input)?;
                Ok(Box::new(PhysicalPlanNode::Projection {
                    input: child,
                    expressions,
                }))
            }
            LogicalPlanNode::Sort { input, exprs } => {
                let child = self.logical_node_to_physical(input)?;
                Ok(Box::new(PhysicalPlanNode::Sort {
                    input: child,
                    sort_exprs: exprs,
                }))
            }
            LogicalPlanNode::Limit {
                input,
                limit,
                offset,
            } => {
                let child = self.logical_node_to_physical(input)?;
                Ok(Box::new(PhysicalPlanNode::Limit {
                    input: child,
                    limit,
                    offset,
                }))
            }
            LogicalPlanNode::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let left_child = self.logical_node_to_physical(left)?;
                let right_child = self.logical_node_to_physical(right)?;

                // For now, always use nested loop join
                // In the future, we'd choose between hash join and nested loop based on statistics
                Ok(Box::new(PhysicalPlanNode::NestedLoopJoin {
                    left: left_child,
                    right: right_child,
                    join_type,
                    condition,
                }))
            }
            LogicalPlanNode::GroupBy {
                input,
                group_exprs,
                aggregate_exprs,
            } => {
                let child = self.logical_node_to_physical(input)?;
                Ok(Box::new(PhysicalPlanNode::HashAggregate {
                    input: child,
                    group_exprs,
                    aggregate_exprs,
                }))
            }
            LogicalPlanNode::Having { input, predicate } => {
                // Having is implemented as a filter after aggregation
                let child = self.logical_node_to_physical(input)?;
                Ok(Box::new(PhysicalPlanNode::Filter {
                    input: child,
                    predicate,
                }))
            }
            LogicalPlanNode::Distinct { input } => {
                let child = self.logical_node_to_physical(input)?;
                // Distinct can be implemented as a group by with all columns
                Ok(Box::new(PhysicalPlanNode::Distinct { input: child }))
            }
            LogicalPlanNode::Values { rows } => Ok(Box::new(PhysicalPlanNode::Values { rows })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::Value;
    use crate::sql::ast::{Expression, OrderByItem, OrderDirection, SelectItem, TableReference};
    use crate::storage::buffer::{lru::LruReplacer, BufferPoolManager};
    use crate::storage::disk::PageManager;
    use tempfile::tempdir;

    fn create_test_planner() -> Result<Planner> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(LruReplacer::new(10));
        let buffer_pool = Arc::new(BufferPoolManager::new(page_manager, replacer, 10));
        let catalog = Arc::new(Catalog::initialize((*buffer_pool).clone())?);

        Ok(Planner::new(catalog))
    }

    #[test]
    fn test_simple_select_planning() -> Result<()> {
        let planner = create_test_planner()?;

        // SELECT id, name FROM users WHERE active = true ORDER BY id DESC LIMIT 10
        let stmt = Statement::Select(SelectStatement {
            distinct: false,
            projections: vec![
                SelectItem::Expression(Expression::column("id"), None),
                SelectItem::Expression(Expression::column("name"), None),
            ],
            from: Some(TableReference {
                name: "users".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: Some(
                Expression::column("active").eq(Expression::literal(Value::Boolean(true))),
            ),
            group_by: vec![],
            having: None,
            order_by: vec![OrderByItem {
                expression: Expression::column("id"),
                direction: OrderDirection::Desc,
            }],
            limit: Some(Expression::literal(Value::Int32(10))),
            offset: None,
        });

        let logical = planner.statement_to_logical(stmt)?;
        match logical {
            LogicalPlan::Select(_) => {
                // Success - we generated a logical plan
            }
            _ => panic!("Expected Select logical plan"),
        }

        Ok(())
    }

    #[test]
    fn test_insert_planning() -> Result<()> {
        let planner = create_test_planner()?;

        // First create the table in catalog
        // Note: We need to mock this since catalog is Arc<Catalog> (immutable)
        // In a real test, we'd use a mutable catalog or mock framework

        // INSERT INTO test_table (id, name) VALUES (1, 'test')
        let stmt = Statement::Insert(InsertStatement {
            table_name: "test_table".to_string(),
            columns: Some(vec!["id".to_string(), "name".to_string()]),
            values: vec![vec![
                Expression::literal(Value::Int32(1)),
                Expression::literal(Value::String("test".to_string())),
            ]],
        });

        let logical = planner.statement_to_logical(stmt)?;
        match logical {
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "test_table");
                assert_eq!(columns, Some(vec!["id".to_string(), "name".to_string()]));
                assert_eq!(values.len(), 1);
            }
            _ => panic!("Expected Insert logical plan"),
        }

        Ok(())
    }

    #[test]
    fn test_create_table_planning() -> Result<()> {
        let planner = create_test_planner()?;

        use crate::sql::ast::{ColumnDefinition, DataType};

        let stmt = Statement::CreateTable(CreateTableStatement {
            table_name: "new_table".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(255)),
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            ],
            constraints: vec![],
        });

        let logical = planner.statement_to_logical(stmt)?;
        match logical {
            LogicalPlan::CreateTable {
                table_name,
                columns,
                ..
            } => {
                assert_eq!(table_name, "new_table");
                assert_eq!(columns.len(), 2);
            }
            _ => panic!("Expected CreateTable logical plan"),
        }

        Ok(())
    }

    #[test]
    fn test_logical_to_physical_conversion() -> Result<()> {
        let planner = create_test_planner()?;

        // Simple SELECT * FROM users
        let logical = LogicalPlan::Select(Box::new(LogicalPlanNode::TableScan {
            table_name: "users".to_string(),
            alias: None,
        }));

        let physical = planner.logical_to_physical(logical)?;
        match physical {
            PhysicalPlan::Select(node) => match *node {
                PhysicalPlanNode::SeqScan { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected SeqScan physical node"),
            },
            _ => panic!("Expected Select physical plan"),
        }

        Ok(())
    }
}
