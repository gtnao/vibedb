// SQL tokens for lexical analysis

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    Identifier(String),
    Number(String),
    String(String),

    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    Drop,
    Alter,
    Index,
    On,
    Primary,
    Key,
    Foreign,
    References,
    And,
    Or,
    Not,
    Null,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    Using,
    As,
    Distinct,
    All,
    Any,
    Some,
    Group,
    Having,
    Union,
    Intersect,
    Except,
    In,
    Exists,
    Between,
    Like,
    Is,
    True,
    False,
    Case,
    When,
    Then,
    Else,
    End,
    Cast,
    If,
    Unique,
    Check,

    // Data types
    Int,
    Integer,
    Bigint,
    Smallint,
    Float,
    Double,
    Decimal,
    Numeric,
    Real,
    Char,
    Varchar,
    Text,
    Date,
    Time,
    Timestamp,
    Boolean,

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Equal,
    NotEqual,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,

    // Delimiters
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Dot,

    // Special
    Eof,
    Whitespace,
    Comment,
}

impl Token {
    /// Check if the token is a keyword
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            Token::Select
                | Token::From
                | Token::Where
                | Token::Insert
                | Token::Into
                | Token::Values
                | Token::Update
                | Token::Set
                | Token::Delete
                | Token::Create
                | Token::Table
                | Token::Drop
                | Token::Alter
                | Token::Index
                | Token::On
                | Token::Primary
                | Token::Key
                | Token::Foreign
                | Token::References
                | Token::And
                | Token::Or
                | Token::Not
                | Token::Null
                | Token::Order
                | Token::By
                | Token::Asc
                | Token::Desc
                | Token::Limit
                | Token::Offset
                | Token::Join
                | Token::Inner
                | Token::Left
                | Token::Right
                | Token::Outer
                | Token::Cross
                | Token::Using
                | Token::As
                | Token::Distinct
                | Token::All
                | Token::Any
                | Token::Some
                | Token::Group
                | Token::Having
                | Token::Union
                | Token::Intersect
                | Token::Except
                | Token::In
                | Token::Exists
                | Token::Between
                | Token::Like
                | Token::Is
                | Token::True
                | Token::False
                | Token::Case
                | Token::When
                | Token::Then
                | Token::Else
                | Token::End
                | Token::Cast
                | Token::If
                | Token::Unique
                | Token::Check
                | Token::Int
                | Token::Integer
                | Token::Bigint
                | Token::Smallint
                | Token::Float
                | Token::Double
                | Token::Decimal
                | Token::Numeric
                | Token::Real
                | Token::Char
                | Token::Varchar
                | Token::Text
                | Token::Date
                | Token::Time
                | Token::Timestamp
                | Token::Boolean
        )
    }

    /// Convert a string to a keyword token if it matches
    pub fn keyword_from_str(s: &str) -> Option<Token> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Token::Select),
            "FROM" => Some(Token::From),
            "WHERE" => Some(Token::Where),
            "INSERT" => Some(Token::Insert),
            "INTO" => Some(Token::Into),
            "VALUES" => Some(Token::Values),
            "UPDATE" => Some(Token::Update),
            "SET" => Some(Token::Set),
            "DELETE" => Some(Token::Delete),
            "CREATE" => Some(Token::Create),
            "TABLE" => Some(Token::Table),
            "DROP" => Some(Token::Drop),
            "ALTER" => Some(Token::Alter),
            "INDEX" => Some(Token::Index),
            "ON" => Some(Token::On),
            "PRIMARY" => Some(Token::Primary),
            "KEY" => Some(Token::Key),
            "FOREIGN" => Some(Token::Foreign),
            "REFERENCES" => Some(Token::References),
            "AND" => Some(Token::And),
            "OR" => Some(Token::Or),
            "NOT" => Some(Token::Not),
            "NULL" => Some(Token::Null),
            "ORDER" => Some(Token::Order),
            "BY" => Some(Token::By),
            "ASC" => Some(Token::Asc),
            "DESC" => Some(Token::Desc),
            "LIMIT" => Some(Token::Limit),
            "OFFSET" => Some(Token::Offset),
            "JOIN" => Some(Token::Join),
            "INNER" => Some(Token::Inner),
            "LEFT" => Some(Token::Left),
            "RIGHT" => Some(Token::Right),
            "OUTER" => Some(Token::Outer),
            "CROSS" => Some(Token::Cross),
            "USING" => Some(Token::Using),
            "AS" => Some(Token::As),
            "DISTINCT" => Some(Token::Distinct),
            "ALL" => Some(Token::All),
            "ANY" => Some(Token::Any),
            "SOME" => Some(Token::Some),
            "GROUP" => Some(Token::Group),
            "HAVING" => Some(Token::Having),
            "UNION" => Some(Token::Union),
            "INTERSECT" => Some(Token::Intersect),
            "EXCEPT" => Some(Token::Except),
            "IN" => Some(Token::In),
            "EXISTS" => Some(Token::Exists),
            "BETWEEN" => Some(Token::Between),
            "LIKE" => Some(Token::Like),
            "IS" => Some(Token::Is),
            "TRUE" => Some(Token::True),
            "FALSE" => Some(Token::False),
            "CASE" => Some(Token::Case),
            "WHEN" => Some(Token::When),
            "THEN" => Some(Token::Then),
            "ELSE" => Some(Token::Else),
            "END" => Some(Token::End),
            "CAST" => Some(Token::Cast),
            "IF" => Some(Token::If),
            "UNIQUE" => Some(Token::Unique),
            "CHECK" => Some(Token::Check),
            "INT" => Some(Token::Int),
            "INTEGER" => Some(Token::Integer),
            "BIGINT" => Some(Token::Bigint),
            "SMALLINT" => Some(Token::Smallint),
            "FLOAT" => Some(Token::Float),
            "DOUBLE" => Some(Token::Double),
            "DECIMAL" => Some(Token::Decimal),
            "NUMERIC" => Some(Token::Numeric),
            "REAL" => Some(Token::Real),
            "CHAR" => Some(Token::Char),
            "VARCHAR" => Some(Token::Varchar),
            "TEXT" => Some(Token::Text),
            "DATE" => Some(Token::Date),
            "TIME" => Some(Token::Time),
            "TIMESTAMP" => Some(Token::Timestamp),
            "BOOLEAN" => Some(Token::Boolean),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_detection() {
        assert!(Token::Select.is_keyword());
        assert!(Token::From.is_keyword());
        assert!(!Token::Identifier("test".to_string()).is_keyword());
        assert!(!Token::Plus.is_keyword());
    }

    #[test]
    fn test_keyword_from_str() {
        assert_eq!(Token::keyword_from_str("SELECT"), Some(Token::Select));
        assert_eq!(Token::keyword_from_str("select"), Some(Token::Select));
        assert_eq!(Token::keyword_from_str("FROM"), Some(Token::From));
        assert_eq!(Token::keyword_from_str("unknown"), None);
    }
}
