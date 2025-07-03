// SQL statement execution planning and optimization

use super::ast::*;
use anyhow::{bail, Result};

/// SQL statement planner - converts AST to execution plan
pub struct StatementPlanner {
    // Future: optimizer hints, statistics, etc.
}

impl StatementPlanner {
    pub fn new() -> Self {
        StatementPlanner {}
    }

    /// Plan a SQL statement for execution
    pub fn plan(&self, statement: Statement) -> Result<ExecutionPlan> {
        match statement {
            Statement::Select(select) => self.plan_select(select),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Update(update) => self.plan_update(update),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::CreateTable(create) => self.plan_create_table(create),
            Statement::DropTable(drop) => self.plan_drop_table(drop),
            Statement::CreateIndex(create) => self.plan_create_index(create),
            Statement::DropIndex(drop) => self.plan_drop_index(drop),
            Statement::BeginTransaction => self.plan_begin_transaction(),
            Statement::Commit => self.plan_commit(),
            Statement::Rollback => self.plan_rollback(),
        }
    }

    fn plan_select(&self, select: SelectStatement) -> Result<ExecutionPlan> {
        // TODO: Implement query planning
        // This would involve:
        // 1. Analyzing available indexes
        // 2. Choosing join order
        // 3. Pushing down predicates
        // 4. Selecting scan methods
        Ok(ExecutionPlan::Select(SelectPlan { statement: select }))
    }

    fn plan_insert(&self, insert: InsertStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::Insert(InsertPlan { statement: insert }))
    }

    fn plan_update(&self, update: UpdateStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::Update(UpdatePlan { statement: update }))
    }

    fn plan_delete(&self, delete: DeleteStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::Delete(DeletePlan { statement: delete }))
    }

    fn plan_create_table(&self, create: CreateTableStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::CreateTable(CreateTablePlan {
            statement: create,
        }))
    }

    fn plan_drop_table(&self, drop: DropTableStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::DropTable(DropTablePlan { statement: drop }))
    }

    fn plan_create_index(&self, create: CreateIndexStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::CreateIndex(CreateIndexPlan {
            statement: create,
        }))
    }

    fn plan_drop_index(&self, drop: DropIndexStatement) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::DropIndex(DropIndexPlan { statement: drop }))
    }

    fn plan_begin_transaction(&self) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::BeginTransaction)
    }

    fn plan_commit(&self) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::Commit)
    }

    fn plan_rollback(&self) -> Result<ExecutionPlan> {
        Ok(ExecutionPlan::Rollback)
    }
}

impl Default for StatementPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Execution plan for a SQL statement
#[derive(Debug, Clone)]
pub enum ExecutionPlan {
    Select(SelectPlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
    CreateIndex(CreateIndexPlan),
    DropIndex(DropIndexPlan),
    BeginTransaction,
    Commit,
    Rollback,
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub statement: SelectStatement,
    // Future: access methods, join order, etc.
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub statement: InsertStatement,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub statement: UpdateStatement,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub statement: DeleteStatement,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub statement: CreateTableStatement,
}

#[derive(Debug, Clone)]
pub struct DropTablePlan {
    pub statement: DropTableStatement,
}

#[derive(Debug, Clone)]
pub struct CreateIndexPlan {
    pub statement: CreateIndexStatement,
}

#[derive(Debug, Clone)]
pub struct DropIndexPlan {
    pub statement: DropIndexStatement,
}

/// Query optimizer - improves execution plans
pub struct QueryOptimizer {
    // Future: cost model, statistics, etc.
}

impl QueryOptimizer {
    pub fn new() -> Self {
        QueryOptimizer {}
    }

    /// Optimize an execution plan
    pub fn optimize(&self, plan: ExecutionPlan) -> Result<ExecutionPlan> {
        // TODO: Implement query optimization
        // This would involve:
        // 1. Predicate pushdown
        // 2. Join reordering
        // 3. Index selection
        // 4. Subquery flattening
        Ok(plan)
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Statement validator - ensures statements are semantically correct
pub struct StatementValidator {
    // Future: schema information
}

impl StatementValidator {
    pub fn new() -> Self {
        StatementValidator {}
    }

    /// Validate a SQL statement
    pub fn validate(&self, statement: &Statement) -> Result<()> {
        match statement {
            Statement::Select(select) => self.validate_select(select),
            Statement::Insert(insert) => self.validate_insert(insert),
            Statement::Update(update) => self.validate_update(update),
            Statement::Delete(delete) => self.validate_delete(delete),
            Statement::CreateTable(create) => self.validate_create_table(create),
            Statement::DropTable(drop) => self.validate_drop_table(drop),
            Statement::CreateIndex(create) => self.validate_create_index(create),
            Statement::DropIndex(drop) => self.validate_drop_index(drop),
            Statement::BeginTransaction => Ok(()), // Transaction statements are always valid
            Statement::Commit => Ok(()),
            Statement::Rollback => Ok(()),
        }
    }

    fn validate_select(&self, _select: &SelectStatement) -> Result<()> {
        // TODO: Validate column references, table names, etc.
        Ok(())
    }

    fn validate_insert(&self, insert: &InsertStatement) -> Result<()> {
        // Validate that all value rows have the same number of columns
        if let Some(first_row) = insert.values.first() {
            let expected_count = first_row.len();
            for (i, row) in insert.values.iter().enumerate() {
                if row.len() != expected_count {
                    bail!(
                        "Row {} has {} values, expected {}",
                        i,
                        row.len(),
                        expected_count
                    );
                }
            }
        }

        // If columns are specified, validate count matches
        if let Some(columns) = &insert.columns {
            if let Some(first_row) = insert.values.first() {
                if columns.len() != first_row.len() {
                    bail!(
                        "Column count ({}) doesn't match value count ({})",
                        columns.len(),
                        first_row.len()
                    );
                }
            }
        }

        Ok(())
    }

    fn validate_update(&self, _update: &UpdateStatement) -> Result<()> {
        // TODO: Validate column references
        Ok(())
    }

    fn validate_delete(&self, _delete: &DeleteStatement) -> Result<()> {
        // TODO: Validate table exists
        Ok(())
    }

    fn validate_create_table(&self, create: &CreateTableStatement) -> Result<()> {
        // Validate no duplicate column names
        let mut seen_columns = std::collections::HashSet::new();
        for column in &create.columns {
            if !seen_columns.insert(&column.name) {
                bail!("Duplicate column name: {}", column.name);
            }
        }

        // Validate constraints reference existing columns
        for constraint in &create.constraints {
            match constraint {
                TableConstraint::PrimaryKey(columns) | TableConstraint::Unique(columns) => {
                    for col in columns {
                        if !create.columns.iter().any(|c| &c.name == col) {
                            bail!("Constraint references non-existent column: {}", col);
                        }
                    }
                }
                TableConstraint::ForeignKey { columns, .. } => {
                    for col in columns {
                        if !create.columns.iter().any(|c| &c.name == col) {
                            bail!("Foreign key references non-existent column: {}", col);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn validate_drop_table(&self, _drop: &DropTableStatement) -> Result<()> {
        // TODO: Validate table exists (unless IF EXISTS)
        Ok(())
    }

    fn validate_create_index(&self, _create: &CreateIndexStatement) -> Result<()> {
        // TODO: Validate table and columns exist
        Ok(())
    }

    fn validate_drop_index(&self, _drop: &DropIndexStatement) -> Result<()> {
        // TODO: Validate index exists (unless IF EXISTS)
        Ok(())
    }
}

impl Default for StatementValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Parser;

    #[test]
    fn test_statement_planner() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let mut parser = Parser::new(sql.to_string());
        let statement = parser.parse().unwrap();

        let planner = StatementPlanner::new();
        let plan = planner.plan(statement).unwrap();

        match plan {
            ExecutionPlan::Select(_) => {}
            _ => panic!("Expected SELECT plan"),
        }
    }

    #[test]
    fn test_statement_validator_valid_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')";
        let mut parser = Parser::new(sql.to_string());
        let statement = parser.parse().unwrap();

        let validator = StatementValidator::new();
        assert!(validator.validate(&statement).is_ok());
    }

    #[test]
    fn test_statement_validator_invalid_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'John'), (2)"; // Missing value
        let mut parser = Parser::new(sql.to_string());
        let statement = parser.parse().unwrap();

        let validator = StatementValidator::new();
        assert!(validator.validate(&statement).is_err());
    }

    #[test]
    fn test_statement_validator_duplicate_columns() {
        let sql = "CREATE TABLE users (id INT, name VARCHAR(50), id INT)"; // Duplicate 'id'
        let mut parser = Parser::new(sql.to_string());
        let statement = parser.parse().unwrap();

        let validator = StatementValidator::new();
        let result = validator.validate(&statement);
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.to_string().contains("Duplicate column name"));
        } else {
            panic!("Expected an error");
        }
    }
}
