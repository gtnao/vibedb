// SQL parser - converts tokens to AST

use super::ast::*;
use super::lexer::Lexer;
use super::token::Token;
use crate::access::Value;
use anyhow::{bail, Result};

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(sql: String) -> Self {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize();
        Parser {
            tokens,
            position: 0,
        }
    }

    /// Parse a SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        match self.current_token() {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Begin => self.parse_begin(),
            Token::Commit => self.parse_commit(),
            Token::Rollback => self.parse_rollback(),
            _ => bail!("Expected SQL statement"),
        }
    }

    /// Parse a SELECT statement
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect_token(Token::Select)?;

        let distinct = if self.match_token(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        // Parse projections
        let projections = self.parse_select_items()?;

        // Parse FROM clause
        let from = if self.match_token(&Token::From) {
            self.advance();
            Some(self.parse_table_reference()?)
        } else {
            None
        };

        // Parse JOINs
        let mut joins = vec![];
        while self.is_join_token() {
            joins.push(self.parse_join()?);
        }

        // Parse WHERE clause
        let where_clause = if self.match_token(&Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY
        let mut group_by = vec![];
        if self.match_token(&Token::Group) {
            self.advance();
            self.expect_token(Token::By)?;
            group_by = self.parse_expression_list()?;
        }

        // Parse HAVING
        let having = if self.match_token(&Token::Having) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY
        let mut order_by = vec![];
        if self.match_token(&Token::Order) {
            self.advance();
            self.expect_token(Token::By)?;
            order_by = self.parse_order_by_items()?;
        }

        // Parse LIMIT
        let limit = if self.match_token(&Token::Limit) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse OFFSET
        let offset = if self.match_token(&Token::Offset) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            distinct,
            projections,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        }))
    }

    /// Parse SELECT items
    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = vec![];

        loop {
            if self.match_token(&Token::Star) {
                self.advance();
                items.push(SelectItem::AllColumns);
            } else if let Token::Identifier(table) = self.current_token() {
                self.advance();
                if self.match_token(&Token::Dot) {
                    self.advance();
                    if self.match_token(&Token::Star) {
                        self.advance();
                        items.push(SelectItem::AllColumnsFrom(table));
                    } else {
                        // It was table.column, need to parse as expression
                        self.position -= 2; // Back up
                        let expr = self.parse_expression()?;
                        let alias = if self.match_token(&Token::As) {
                            self.advance();
                            Some(self.expect_identifier()?)
                        } else if let Token::Identifier(alias) = self.current_token() {
                            self.advance();
                            Some(alias)
                        } else {
                            None
                        };
                        items.push(SelectItem::Expression(expr, alias));
                    }
                } else {
                    // Just a column or expression starting with identifier
                    self.position -= 1; // Back up
                    let expr = self.parse_expression()?;
                    let alias = if self.match_token(&Token::As) {
                        self.advance();
                        Some(self.expect_identifier()?)
                    } else if let Token::Identifier(alias) = self.current_token() {
                        self.advance();
                        Some(alias)
                    } else {
                        None
                    };
                    items.push(SelectItem::Expression(expr, alias));
                }
            } else {
                let expr = self.parse_expression()?;
                let alias = if self.match_token(&Token::As) {
                    self.advance();
                    Some(self.expect_identifier()?)
                } else if let Token::Identifier(alias) = self.current_token() {
                    self.advance();
                    Some(alias)
                } else {
                    None
                };
                items.push(SelectItem::Expression(expr, alias));
            }

            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(items)
    }

    /// Parse table reference
    fn parse_table_reference(&mut self) -> Result<TableReference> {
        let name = self.expect_identifier()?;
        let alias = if self.match_token(&Token::As) {
            self.advance();
            Some(self.expect_identifier()?)
        } else if let Token::Identifier(alias) = self.current_token() {
            self.advance();
            Some(alias)
        } else {
            None
        };

        Ok(TableReference { name, alias })
    }

    /// Check if current token is a JOIN token
    fn is_join_token(&self) -> bool {
        matches!(
            self.current_token(),
            Token::Join | Token::Inner | Token::Left | Token::Right | Token::Cross
        )
    }

    /// Parse JOIN clause
    fn parse_join(&mut self) -> Result<Join> {
        let join_type = match self.current_token() {
            Token::Join => {
                self.advance();
                JoinType::Inner
            }
            Token::Inner => {
                self.advance();
                self.expect_token(Token::Join)?;
                JoinType::Inner
            }
            Token::Left => {
                self.advance();
                self.match_token(&Token::Outer); // Optional OUTER
                self.expect_token(Token::Join)?;
                JoinType::Left
            }
            Token::Right => {
                self.advance();
                self.match_token(&Token::Outer); // Optional OUTER
                self.expect_token(Token::Join)?;
                JoinType::Right
            }
            Token::Cross => {
                self.advance();
                self.expect_token(Token::Join)?;
                JoinType::Cross
            }
            _ => bail!("Expected JOIN keyword"),
        };

        let table = self.parse_table_reference()?;

        let (on, using) = if self.match_token(&Token::On) {
            self.advance();
            (Some(self.parse_expression()?), vec![])
        } else if self.match_token(&Token::Using) {
            self.advance();
            self.expect_token(Token::LeftParen)?;
            let columns = self.parse_identifier_list()?;
            self.expect_token(Token::RightParen)?;
            (None, columns)
        } else {
            (None, vec![])
        };

        Ok(Join {
            join_type,
            table,
            on,
            using,
        })
    }

    /// Parse ORDER BY items
    fn parse_order_by_items(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = vec![];

        loop {
            let expression = self.parse_expression()?;
            let direction = if self.match_token(&Token::Asc) {
                self.advance();
                OrderDirection::Asc
            } else if self.match_token(&Token::Desc) {
                self.advance();
                OrderDirection::Desc
            } else {
                OrderDirection::Asc
            };

            items.push(OrderByItem {
                expression,
                direction,
            });

            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(items)
    }

    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect_token(Token::Insert)?;
        self.expect_token(Token::Into)?;

        let table_name = self.expect_identifier()?;

        let columns = if self.match_token(&Token::LeftParen) {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect_token(Token::Values)?;

        let mut values = vec![];
        loop {
            self.expect_token(Token::LeftParen)?;
            let row = self.parse_expression_list()?;
            self.expect_token(Token::RightParen)?;
            values.push(row);

            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(Statement::Insert(InsertStatement {
            table_name,
            columns,
            values,
        }))
    }

    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect_token(Token::Update)?;

        let table_name = self.expect_identifier()?;

        self.expect_token(Token::Set)?;

        let assignments = self.parse_assignments()?;

        let where_clause = if self.match_token(&Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table_name,
            assignments,
            where_clause,
        }))
    }

    /// Parse assignments for UPDATE
    fn parse_assignments(&mut self) -> Result<Vec<Assignment>> {
        let mut assignments = vec![];

        loop {
            let column = self.expect_identifier()?;
            self.expect_token(Token::Equal)?;
            let value = self.parse_expression()?;

            assignments.push(Assignment { column, value });

            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(assignments)
    }

    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect_token(Token::Delete)?;
        self.expect_token(Token::From)?;

        let table_name = self.expect_identifier()?;

        let where_clause = if self.match_token(&Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table_name,
            where_clause,
        }))
    }

    /// Parse CREATE statement
    fn parse_create(&mut self) -> Result<Statement> {
        self.expect_token(Token::Create)?;

        match self.current_token() {
            Token::Table => self.parse_create_table(),
            Token::Index => self.parse_create_index(),
            _ => bail!("Expected TABLE or INDEX after CREATE"),
        }
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect_token(Token::Table)?;

        let table_name = self.expect_identifier()?;

        self.expect_token(Token::LeftParen)?;

        let mut columns = vec![];
        let mut constraints = vec![];

        loop {
            // Check if it's a table constraint
            if self.is_table_constraint() {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_definition()?);
            }

            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        self.expect_token(Token::RightParen)?;

        Ok(Statement::CreateTable(CreateTableStatement {
            table_name,
            columns,
            constraints,
        }))
    }

    /// Check if current position is a table constraint
    fn is_table_constraint(&self) -> bool {
        matches!(
            self.current_token(),
            Token::Primary | Token::Foreign | Token::Unique | Token::Check
        )
    }

    /// Parse column definition
    fn parse_column_definition(&mut self) -> Result<ColumnDefinition> {
        let name = self.expect_identifier()?;
        let data_type = self.parse_data_type()?;

        let mut nullable = true;
        let mut default = None;
        let mut constraints = vec![];

        // Parse column constraints
        loop {
            match self.current_token() {
                Token::Not => {
                    self.advance();
                    self.expect_token(Token::Null)?;
                    nullable = false;
                    constraints.push(ColumnConstraint::NotNull);
                }
                Token::Null => {
                    self.advance();
                    nullable = true;
                }
                Token::Primary => {
                    self.advance();
                    self.expect_token(Token::Key)?;
                    constraints.push(ColumnConstraint::PrimaryKey);
                }
                Token::Unique => {
                    self.advance();
                    constraints.push(ColumnConstraint::Unique);
                }
                Token::Check => {
                    self.advance();
                    self.expect_token(Token::LeftParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RightParen)?;
                    constraints.push(ColumnConstraint::Check(expr));
                }
                Token::References => {
                    self.advance();
                    let table = self.expect_identifier()?;
                    let column = if self.match_token(&Token::LeftParen) {
                        self.advance();
                        let col = self.expect_identifier()?;
                        self.expect_token(Token::RightParen)?;
                        Some(col)
                    } else {
                        None
                    };

                    let (on_delete, on_update) = self.parse_referential_actions()?;

                    constraints.push(ColumnConstraint::References {
                        table,
                        column,
                        on_delete,
                        on_update,
                    });
                }
                Token::Identifier(s) if s.to_uppercase() == "DEFAULT" => {
                    self.advance();
                    default = Some(self.parse_expression()?);
                }
                _ => break,
            }
        }

        Ok(ColumnDefinition {
            name,
            data_type,
            nullable,
            default,
            constraints,
        })
    }

    /// Parse data type
    fn parse_data_type(&mut self) -> Result<DataType> {
        let data_type = match self.current_token() {
            Token::Int | Token::Integer => {
                self.advance();
                DataType::Int
            }
            Token::Bigint => {
                self.advance();
                DataType::BigInt
            }
            Token::Smallint => {
                self.advance();
                DataType::SmallInt
            }
            Token::Float => {
                self.advance();
                DataType::Float
            }
            Token::Double => {
                self.advance();
                DataType::Double
            }
            Token::Decimal | Token::Numeric => {
                self.advance();
                let (precision, scale) = if self.match_token(&Token::LeftParen) {
                    self.advance();
                    let p = self
                        .expect_number()?
                        .parse::<u8>()
                        .map_err(|e| anyhow::anyhow!("Invalid precision: {}", e))?;
                    let s = if self.match_token(&Token::Comma) {
                        self.advance();
                        Some(
                            self.expect_number()?
                                .parse::<u8>()
                                .map_err(|e| anyhow::anyhow!("Invalid scale: {}", e))?,
                        )
                    } else {
                        None
                    };
                    self.expect_token(Token::RightParen)?;
                    (Some(p), s)
                } else {
                    (None, None)
                };
                DataType::Decimal(precision, scale)
            }
            Token::Char => {
                self.advance();
                let length = if self.match_token(&Token::LeftParen) {
                    self.advance();
                    let len = self
                        .expect_number()?
                        .parse::<u32>()
                        .map_err(|e| anyhow::anyhow!("Invalid length: {}", e))?;
                    self.expect_token(Token::RightParen)?;
                    Some(len)
                } else {
                    None
                };
                DataType::Char(length)
            }
            Token::Varchar => {
                self.advance();
                let length = if self.match_token(&Token::LeftParen) {
                    self.advance();
                    let len = self
                        .expect_number()?
                        .parse::<u32>()
                        .map_err(|e| anyhow::anyhow!("Invalid length: {}", e))?;
                    self.expect_token(Token::RightParen)?;
                    Some(len)
                } else {
                    None
                };
                DataType::Varchar(length)
            }
            Token::Text => {
                self.advance();
                DataType::Text
            }
            Token::Date => {
                self.advance();
                DataType::Date
            }
            Token::Time => {
                self.advance();
                DataType::Time
            }
            Token::Timestamp => {
                self.advance();
                DataType::Timestamp
            }
            Token::Boolean => {
                self.advance();
                DataType::Boolean
            }
            _ => bail!("Expected data type"),
        };

        Ok(data_type)
    }

    /// Parse referential actions (ON DELETE, ON UPDATE)
    fn parse_referential_actions(
        &mut self,
    ) -> Result<(Option<ReferentialAction>, Option<ReferentialAction>)> {
        let mut on_delete = None;
        let mut on_update = None;

        while self.match_token(&Token::On) {
            self.advance();
            match self.current_token() {
                Token::Delete => {
                    self.advance();
                    on_delete = Some(self.parse_referential_action()?);
                }
                Token::Update => {
                    self.advance();
                    on_update = Some(self.parse_referential_action()?);
                }
                _ => bail!("Expected DELETE or UPDATE after ON"),
            }
        }

        Ok((on_delete, on_update))
    }

    /// Parse single referential action
    fn parse_referential_action(&mut self) -> Result<ReferentialAction> {
        match self.current_token() {
            Token::Identifier(s) => {
                let action = match s.to_uppercase().as_str() {
                    "CASCADE" => ReferentialAction::Cascade,
                    "RESTRICT" => ReferentialAction::Restrict,
                    _ => bail!("Invalid referential action: {}", s),
                };
                self.advance();
                Ok(action)
            }
            Token::Set => {
                self.advance();
                match self.current_token() {
                    Token::Null => {
                        self.advance();
                        Ok(ReferentialAction::SetNull)
                    }
                    Token::Identifier(s) if s.to_uppercase() == "DEFAULT" => {
                        self.advance();
                        Ok(ReferentialAction::SetDefault)
                    }
                    _ => bail!("Expected NULL or DEFAULT after SET"),
                }
            }
            _ => bail!("Expected referential action"),
        }
    }

    /// Parse table constraint
    fn parse_table_constraint(&mut self) -> Result<TableConstraint> {
        match self.current_token() {
            Token::Primary => {
                self.advance();
                self.expect_token(Token::Key)?;
                self.expect_token(Token::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect_token(Token::RightParen)?;
                Ok(TableConstraint::PrimaryKey(columns))
            }
            Token::Foreign => {
                self.advance();
                self.expect_token(Token::Key)?;
                self.expect_token(Token::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect_token(Token::RightParen)?;
                self.expect_token(Token::References)?;
                let ref_table = self.expect_identifier()?;
                self.expect_token(Token::LeftParen)?;
                let ref_columns = self.parse_identifier_list()?;
                self.expect_token(Token::RightParen)?;

                let (on_delete, on_update) = self.parse_referential_actions()?;

                Ok(TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                })
            }
            Token::Unique => {
                self.advance();
                self.expect_token(Token::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect_token(Token::RightParen)?;
                Ok(TableConstraint::Unique(columns))
            }
            Token::Check => {
                self.advance();
                self.expect_token(Token::LeftParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RightParen)?;
                Ok(TableConstraint::Check(expr))
            }
            _ => bail!("Expected table constraint"),
        }
    }

    /// Parse CREATE INDEX statement
    fn parse_create_index(&mut self) -> Result<Statement> {
        self.expect_token(Token::Index)?;

        let index_name = self.expect_identifier()?;

        self.expect_token(Token::On)?;

        let table_name = self.expect_identifier()?;

        self.expect_token(Token::LeftParen)?;
        let columns = self.parse_identifier_list()?;
        self.expect_token(Token::RightParen)?;

        Ok(Statement::CreateIndex(CreateIndexStatement {
            index_name,
            table_name,
            columns,
            unique: false, // TODO: Support UNIQUE indexes
        }))
    }

    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect_token(Token::Drop)?;

        match self.current_token() {
            Token::Table => self.parse_drop_table(),
            Token::Index => self.parse_drop_index(),
            _ => bail!("Expected TABLE or INDEX after DROP"),
        }
    }

    /// Parse DROP TABLE statement
    fn parse_drop_table(&mut self) -> Result<Statement> {
        self.expect_token(Token::Table)?;

        let if_exists = if self.match_token(&Token::If) {
            self.advance();
            self.expect_token(Token::Exists)?;
            true
        } else {
            false
        };

        let table_name = self.expect_identifier()?;

        Ok(Statement::DropTable(DropTableStatement {
            table_name,
            if_exists,
        }))
    }

    /// Parse DROP INDEX statement
    fn parse_drop_index(&mut self) -> Result<Statement> {
        self.expect_token(Token::Index)?;

        let if_exists = if self.match_token(&Token::If) {
            self.advance();
            self.expect_token(Token::Exists)?;
            true
        } else {
            false
        };

        let index_name = self.expect_identifier()?;

        Ok(Statement::DropIndex(DropIndexStatement {
            index_name,
            if_exists,
        }))
    }

    /// Parse BEGIN [TRANSACTION|WORK]
    fn parse_begin(&mut self) -> Result<Statement> {
        self.expect_token(Token::Begin)?;
        
        // Optional TRANSACTION or WORK keyword
        if self.match_token(&Token::Transaction) || self.match_token(&Token::Work) {
            self.advance();
        }
        
        Ok(Statement::BeginTransaction)
    }

    /// Parse COMMIT [TRANSACTION|WORK]
    fn parse_commit(&mut self) -> Result<Statement> {
        self.expect_token(Token::Commit)?;
        
        // Optional TRANSACTION or WORK keyword
        if self.match_token(&Token::Transaction) || self.match_token(&Token::Work) {
            self.advance();
        }
        
        Ok(Statement::Commit)
    }

    /// Parse ROLLBACK [TRANSACTION|WORK]
    fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect_token(Token::Rollback)?;
        
        // Optional TRANSACTION or WORK keyword
        if self.match_token(&Token::Transaction) || self.match_token(&Token::Work) {
            self.advance();
        }
        
        Ok(Statement::Rollback)
    }

    /// Parse expression
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or()
    }

    /// Parse OR expression
    fn parse_or(&mut self) -> Result<Expression> {
        let mut left = self.parse_and()?;

        while self.match_token(&Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and(&mut self) -> Result<Expression> {
        let mut left = self.parse_not()?;

        while self.match_token(&Token::And) {
            self.advance();
            let right = self.parse_not()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse NOT expression
    fn parse_not(&mut self) -> Result<Expression> {
        if self.match_token(&Token::Not) {
            self.advance();
            let operand = self.parse_comparison()?;
            Ok(Expression::UnaryOp {
                op: UnaryOperator::Not,
                operand: Box::new(operand),
            })
        } else {
            self.parse_comparison()
        }
    }

    /// Parse comparison expression
    fn parse_comparison(&mut self) -> Result<Expression> {
        let left = self.parse_addition()?;

        // Handle special comparison operators
        if self.match_token(&Token::Is) {
            self.advance();
            let negated = if self.match_token(&Token::Not) {
                self.advance();
                true
            } else {
                false
            };
            self.expect_token(Token::Null)?;
            return Ok(Expression::IsNull {
                expression: Box::new(left),
                negated,
            });
        }

        if self.match_token(&Token::In) {
            self.advance();
            self.expect_token(Token::LeftParen)?;
            let list = self.parse_expression_list()?;
            self.expect_token(Token::RightParen)?;
            return Ok(Expression::InList {
                expression: Box::new(left),
                list,
                negated: false,
            });
        }

        if self.match_token(&Token::Between) {
            self.advance();
            let low = self.parse_addition()?;
            self.expect_token(Token::And)?;
            let high = self.parse_addition()?;
            return Ok(Expression::Between {
                expression: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            });
        }

        if self.match_token(&Token::Like) {
            self.advance();
            let pattern = self.parse_addition()?;
            let escape = if self.match_token(&Token::Identifier(String::from("ESCAPE"))) {
                self.advance();
                Some(Box::new(self.parse_addition()?))
            } else {
                None
            };
            return Ok(Expression::Like {
                expression: Box::new(left),
                pattern: Box::new(pattern),
                escape,
                negated: false,
            });
        }

        // Standard comparison operators
        let op = match self.current_token() {
            Token::Equal => {
                self.advance();
                Some(BinaryOperator::Equal)
            }
            Token::NotEqual => {
                self.advance();
                Some(BinaryOperator::NotEqual)
            }
            Token::Less => {
                self.advance();
                Some(BinaryOperator::Less)
            }
            Token::Greater => {
                self.advance();
                Some(BinaryOperator::Greater)
            }
            Token::LessEqual => {
                self.advance();
                Some(BinaryOperator::LessEqual)
            }
            Token::GreaterEqual => {
                self.advance();
                Some(BinaryOperator::GreaterEqual)
            }
            _ => None,
        };

        if let Some(op) = op {
            let right = self.parse_addition()?;
            Ok(Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    /// Parse addition/subtraction expression
    fn parse_addition(&mut self) -> Result<Expression> {
        let mut left = self.parse_multiplication()?;

        loop {
            let op = match self.current_token() {
                Token::Plus => {
                    self.advance();
                    BinaryOperator::Plus
                }
                Token::Minus => {
                    self.advance();
                    BinaryOperator::Minus
                }
                _ => break,
            };

            let right = self.parse_multiplication()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse multiplication/division expression
    fn parse_multiplication(&mut self) -> Result<Expression> {
        let mut left = self.parse_unary()?;

        loop {
            let op = match self.current_token() {
                Token::Star => {
                    self.advance();
                    BinaryOperator::Multiply
                }
                Token::Slash => {
                    self.advance();
                    BinaryOperator::Divide
                }
                Token::Percent => {
                    self.advance();
                    BinaryOperator::Modulo
                }
                _ => break,
            };

            let right = self.parse_unary()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse unary expression
    fn parse_unary(&mut self) -> Result<Expression> {
        match self.current_token() {
            Token::Plus => {
                self.advance();
                let operand = self.parse_unary()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Plus,
                    operand: Box::new(operand),
                })
            }
            Token::Minus => {
                self.advance();
                let operand = self.parse_unary()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    operand: Box::new(operand),
                })
            }
            _ => self.parse_primary(),
        }
    }

    /// Parse primary expression
    fn parse_primary(&mut self) -> Result<Expression> {
        match self.current_token() {
            Token::Number(n) => {
                self.advance();
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i32>() {
                    Ok(Expression::Literal(Value::Int32(i)))
                } else if let Ok(_f) = n.parse::<f64>() {
                    // TODO: Add float support when Value type supports it
                    bail!("Float values not yet supported")
                } else {
                    bail!("Invalid number: {}", n)
                }
            }
            Token::String(s) => {
                self.advance();
                Ok(Expression::Literal(Value::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expression::Literal(Value::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expression::Literal(Value::Boolean(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expression::Null)
            }
            Token::Identifier(name) => {
                self.advance();

                // Check for qualified column (table.column)
                if self.match_token(&Token::Dot) {
                    self.advance();
                    let column = self.expect_identifier()?;
                    Ok(Expression::QualifiedColumn(name, column))
                }
                // Check for function call
                else if self.match_token(&Token::LeftParen) {
                    self.advance();

                    // Check for DISTINCT in aggregate functions
                    let distinct = if self.match_token(&Token::Distinct) {
                        self.advance();
                        true
                    } else {
                        false
                    };

                    let args = if self.match_token(&Token::RightParen) {
                        vec![]
                    } else {
                        let args = self.parse_expression_list()?;
                        self.expect_token(Token::RightParen)?;
                        args
                    };

                    Ok(Expression::Function {
                        name,
                        args,
                        distinct,
                    })
                } else {
                    Ok(Expression::Column(name))
                }
            }
            Token::LeftParen => {
                self.advance();

                // Check if it's a subquery
                if matches!(self.current_token(), Token::Select) {
                    let subquery = match self.parse_select()? {
                        Statement::Select(s) => s,
                        _ => unreachable!(),
                    };
                    self.expect_token(Token::RightParen)?;
                    Ok(Expression::Subquery(Box::new(subquery)))
                } else {
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RightParen)?;
                    Ok(expr)
                }
            }
            Token::Case => self.parse_case_expression(),
            Token::Cast => self.parse_cast_expression(),
            _ => bail!("Unexpected token: {:?}", self.current_token()),
        }
    }

    /// Parse CASE expression
    fn parse_case_expression(&mut self) -> Result<Expression> {
        self.expect_token(Token::Case)?;

        // Check if there's an operand
        let operand = if !self.match_token(&Token::When) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        let mut when_clauses = vec![];

        while self.match_token(&Token::When) {
            self.advance();
            let condition = self.parse_expression()?;
            self.expect_token(Token::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push(WhenClause { condition, result });
        }

        let else_clause = if self.match_token(&Token::Else) {
            self.advance();
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect_token(Token::End)?;

        Ok(Expression::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    /// Parse CAST expression
    fn parse_cast_expression(&mut self) -> Result<Expression> {
        self.expect_token(Token::Cast)?;
        self.expect_token(Token::LeftParen)?;

        let expression = self.parse_expression()?;

        self.expect_token(Token::As)?;

        let data_type = self.parse_data_type()?;

        self.expect_token(Token::RightParen)?;

        Ok(Expression::Cast {
            expression: Box::new(expression),
            data_type,
        })
    }

    /// Parse list of expressions
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut expressions = vec![];

        loop {
            expressions.push(self.parse_expression()?);
            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(expressions)
    }

    /// Parse list of identifiers
    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = vec![];

        loop {
            identifiers.push(self.expect_identifier()?);
            if !self.match_token(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(identifiers)
    }

    // Helper methods

    /// Get current token
    fn current_token(&self) -> Token {
        self.tokens
            .get(self.position)
            .cloned()
            .unwrap_or(Token::Eof)
    }

    /// Advance to next token
    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }

    /// Check if current token matches
    fn match_token(&self, token: &Token) -> bool {
        self.current_token() == *token
    }

    /// Expect a specific token
    fn expect_token(&mut self, token: Token) -> Result<()> {
        if self.current_token() == token {
            self.advance();
            Ok(())
        } else {
            bail!("Expected {:?}, found {:?}", token, self.current_token())
        }
    }

    /// Expect an identifier
    fn expect_identifier(&mut self) -> Result<String> {
        match self.current_token() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            _ => bail!("Expected identifier"),
        }
    }

    /// Expect a number
    fn expect_number(&mut self) -> Result<String> {
        match self.current_token() {
            Token::Number(n) => {
                self.advance();
                Ok(n)
            }
            _ => bail!("Expected number"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_simple() {
        let mut parser = Parser::new("SELECT id, name FROM users".to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.projections.len(), 2);
                assert!(select.from.is_some());
                assert_eq!(select.from.unwrap().name, "users");
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let mut parser =
            Parser::new("SELECT * FROM users WHERE age > 18 AND status = 'active'".to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser =
            Parser::new("INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')".to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table_name, "users");
                assert_eq!(
                    insert.columns,
                    Some(vec!["id".to_string(), "name".to_string()])
                );
                assert_eq!(insert.values.len(), 2);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut parser =
            Parser::new("UPDATE users SET name = 'John Doe', age = 30 WHERE id = 1".to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Update(update) => {
                assert_eq!(update.table_name, "users");
                assert_eq!(update.assignments.len(), 2);
                assert!(update.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1".to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.table_name, "users");
                assert!(delete.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE,
            age INT CHECK (age >= 0)
        )";

        let mut parser = Parser::new(sql.to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.columns.len(), 4);

                // Check first column
                let id_col = &create.columns[0];
                assert_eq!(id_col.name, "id");
                assert!(matches!(id_col.data_type, DataType::Int));
                assert!(id_col.constraints.contains(&ColumnConstraint::PrimaryKey));
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_join() {
        let sql = "SELECT u.id, p.title 
                   FROM users u 
                   INNER JOIN posts p ON u.id = p.user_id
                   WHERE u.active = true";

        let mut parser = Parser::new(sql.to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.from.as_ref().unwrap().name, "users");
                assert_eq!(select.from.as_ref().unwrap().alias, Some("u".to_string()));
                assert_eq!(select.joins.len(), 1);

                let join = &select.joins[0];
                assert!(matches!(join.join_type, JoinType::Inner));
                assert_eq!(join.table.name, "posts");
                assert_eq!(join.table.alias, Some("p".to_string()));
                assert!(join.on.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_complex_expression() {
        let sql =
            "SELECT * FROM users WHERE (age > 18 AND age < 65) OR status IN ('active', 'premium')";

        let mut parser = Parser::new(sql.to_string());
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }
}
