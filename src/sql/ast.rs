// SQL Abstract Syntax Tree (AST) definitions

use crate::access::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub distinct: bool,
    pub projections: Vec<SelectItem>,
    pub from: Option<TableReference>,
    pub joins: Vec<Join>,
    pub where_clause: Option<Expression>,
    pub group_by: Vec<Expression>,
    pub having: Option<Expression>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    AllColumns,
    AllColumnsFrom(String),
    Expression(Expression, Option<String>), // expression, alias
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableReference {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub table: TableReference,
    pub on: Option<Expression>,
    pub using: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expression: Expression,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expression>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Expression>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    BigInt,
    SmallInt,
    Float,
    Double,
    Decimal(Option<u8>, Option<u8>), // precision, scale
    Char(Option<u32>),
    Varchar(Option<u32>),
    Text,
    Date,
    Time,
    Timestamp,
    Boolean,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    NotNull,
    Unique,
    Check(Expression),
    References {
        table: String,
        column: Option<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Unique(Vec<String>),
    Check(Expression),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    pub index_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    // Literals
    Literal(Value),

    // Null literal
    Null,

    // Column reference
    Column(String),
    QualifiedColumn(String, String), // table.column

    // Binary operations
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },

    // Unary operations
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },

    // Function call
    Function {
        name: String,
        args: Vec<Expression>,
        distinct: bool,
    },

    // CASE expression
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expression>>,
    },

    // CAST expression
    Cast {
        expression: Box<Expression>,
        data_type: DataType,
    },

    // IN expression
    InList {
        expression: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },

    // BETWEEN expression
    Between {
        expression: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    },

    // EXISTS expression
    Exists {
        subquery: Box<SelectStatement>,
        negated: bool,
    },

    // LIKE expression
    Like {
        expression: Box<Expression>,
        pattern: Box<Expression>,
        escape: Option<Box<Expression>>,
        negated: bool,
    },

    // IS NULL expression
    IsNull {
        expression: Box<Expression>,
        negated: bool,
    },

    // Subquery
    Subquery(Box<SelectStatement>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expression,
    pub result: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,

    // Logical
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

impl Expression {
    /// Create a literal expression from a value
    pub fn literal(value: Value) -> Self {
        Expression::Literal(value)
    }

    /// Create a column reference expression
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(name.into())
    }

    /// Create a qualified column reference (table.column)
    pub fn qualified_column(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expression::QualifiedColumn(table.into(), column.into())
    }

    /// Create an equality comparison
    pub fn eq(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Equal,
            right: Box::new(other),
        }
    }

    /// Create a greater than comparison
    pub fn gt(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Greater,
            right: Box::new(other),
        }
    }

    /// Create an AND expression
    pub fn and(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::And,
            right: Box::new(other),
        }
    }

    /// Create an OR expression
    pub fn or(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Or,
            right: Box::new(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_builders() {
        let expr = Expression::column("age")
            .gt(Expression::literal(Value::Int32(18)))
            .and(
                Expression::column("status")
                    .eq(Expression::literal(Value::String("active".to_string()))),
            );

        match expr {
            Expression::BinaryOp {
                op: BinaryOperator::And,
                ..
            } => {}
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_select_statement() {
        let stmt = SelectStatement {
            distinct: false,
            projections: vec![
                SelectItem::Expression(Expression::column("id"), None),
                SelectItem::Expression(Expression::column("name"), Some("user_name".to_string())),
            ],
            from: Some(TableReference {
                name: "users".to_string(),
                alias: Some("u".to_string()),
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
        };

        assert_eq!(stmt.projections.len(), 2);
        assert!(stmt.where_clause.is_some());
        assert_eq!(stmt.order_by.len(), 1);
    }
}
