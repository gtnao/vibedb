// SQL lexer - tokenizes SQL statements

use super::token::Token;

pub struct Lexer {
    input: String,
    position: usize,
    current_char: Option<char>,
}

impl Lexer {
    pub fn new(input: String) -> Self {
        let mut lexer = Lexer {
            input,
            position: 0,
            current_char: None,
        };
        lexer.current_char = lexer.input.chars().next();
        lexer
    }

    /// Get the next token from the input
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        if self.current_char.is_none() {
            return Token::Eof;
        }

        let token = match self.current_char.unwrap() {
            '+' => {
                self.advance();
                Token::Plus
            }
            '-' => {
                self.advance();
                // Check for comments
                if self.current_char == Some('-') {
                    self.skip_comment();
                    return self.next_token();
                }
                Token::Minus
            }
            '*' => {
                self.advance();
                Token::Star
            }
            '/' => {
                self.advance();
                Token::Slash
            }
            '%' => {
                self.advance();
                Token::Percent
            }
            '=' => {
                self.advance();
                Token::Equal
            }
            '<' => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::LessEqual
                } else if self.current_char == Some('>') {
                    self.advance();
                    Token::NotEqual
                } else {
                    Token::Less
                }
            }
            '>' => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::GreaterEqual
                } else {
                    Token::Greater
                }
            }
            '!' => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::NotEqual
                } else {
                    // Invalid token, but we'll handle it as identifier
                    Token::Identifier("!".to_string())
                }
            }
            '(' => {
                self.advance();
                Token::LeftParen
            }
            ')' => {
                self.advance();
                Token::RightParen
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            ';' => {
                self.advance();
                Token::Semicolon
            }
            '.' => {
                self.advance();
                Token::Dot
            }
            '\'' => self.read_string(),
            '"' => self.read_quoted_identifier(),
            c if c.is_alphabetic() || c == '_' => self.read_identifier(),
            c if c.is_numeric() => self.read_number(),
            _ => {
                self.advance();
                Token::Identifier(self.current_char.unwrap_or(' ').to_string())
            }
        };

        token
    }

    /// Advance to the next character
    fn advance(&mut self) {
        self.position += 1;
        if self.position < self.input.len() {
            self.current_char = self.input.chars().nth(self.position);
        } else {
            self.current_char = None;
        }
    }

    /// Peek at the next character without advancing
    fn peek(&self) -> Option<char> {
        if self.position + 1 < self.input.len() {
            self.input.chars().nth(self.position + 1)
        } else {
            None
        }
    }

    /// Skip whitespace characters
    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current_char {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Skip single-line comments starting with --
    fn skip_comment(&mut self) {
        while let Some(ch) = self.current_char {
            if ch == '\n' {
                self.advance();
                break;
            }
            self.advance();
        }
    }

    /// Read an identifier or keyword
    fn read_identifier(&mut self) -> Token {
        let mut identifier = String::new();

        while let Some(ch) = self.current_char {
            if ch.is_alphanumeric() || ch == '_' {
                identifier.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        // Check if it's a keyword
        Token::keyword_from_str(&identifier).unwrap_or_else(|| Token::Identifier(identifier))
    }

    /// Read a quoted identifier (e.g., "table name")
    fn read_quoted_identifier(&mut self) -> Token {
        self.advance(); // Skip opening quote
        let mut identifier = String::new();

        while let Some(ch) = self.current_char {
            if ch == '"' {
                self.advance(); // Skip closing quote
                break;
            } else if ch == '\\' && self.peek() == Some('"') {
                // Handle escaped quotes
                self.advance();
                identifier.push('"');
                self.advance();
            } else {
                identifier.push(ch);
                self.advance();
            }
        }

        Token::Identifier(identifier)
    }

    /// Read a string literal
    fn read_string(&mut self) -> Token {
        self.advance(); // Skip opening quote
        let mut string = String::new();

        while let Some(ch) = self.current_char {
            if ch == '\'' {
                if self.peek() == Some('\'') {
                    // Handle escaped single quotes
                    string.push('\'');
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // Skip closing quote
                    break;
                }
            } else {
                string.push(ch);
                self.advance();
            }
        }

        Token::String(string)
    }

    /// Read a number (integer or float)
    fn read_number(&mut self) -> Token {
        let mut number = String::new();
        let mut has_dot = false;

        while let Some(ch) = self.current_char {
            if ch.is_numeric() {
                number.push(ch);
                self.advance();
            } else if ch == '.' && !has_dot && self.peek().map_or(false, |c| c.is_numeric()) {
                has_dot = true;
                number.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        Token::Number(number)
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token();
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }

        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokens() {
        let mut lexer = Lexer::new("SELECT * FROM table".to_string());
        assert_eq!(lexer.next_token(), Token::Select);
        assert_eq!(lexer.next_token(), Token::Star);
        assert_eq!(lexer.next_token(), Token::From);
        assert_eq!(lexer.next_token(), Token::Table);
        assert_eq!(lexer.next_token(), Token::Eof);
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("+ - * / % = < > <= >= <> !=".to_string());
        assert_eq!(lexer.next_token(), Token::Plus);
        assert_eq!(lexer.next_token(), Token::Minus);
        assert_eq!(lexer.next_token(), Token::Star);
        assert_eq!(lexer.next_token(), Token::Slash);
        assert_eq!(lexer.next_token(), Token::Percent);
        assert_eq!(lexer.next_token(), Token::Equal);
        assert_eq!(lexer.next_token(), Token::Less);
        assert_eq!(lexer.next_token(), Token::Greater);
        assert_eq!(lexer.next_token(), Token::LessEqual);
        assert_eq!(lexer.next_token(), Token::GreaterEqual);
        assert_eq!(lexer.next_token(), Token::NotEqual);
        assert_eq!(lexer.next_token(), Token::NotEqual);
    }

    #[test]
    fn test_string_literals() {
        let mut lexer = Lexer::new("'hello world' 'it''s fine'".to_string());
        assert_eq!(lexer.next_token(), Token::String("hello world".to_string()));
        assert_eq!(lexer.next_token(), Token::String("it's fine".to_string()));
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("123 456.789 0.5".to_string());
        assert_eq!(lexer.next_token(), Token::Number("123".to_string()));
        assert_eq!(lexer.next_token(), Token::Number("456.789".to_string()));
        assert_eq!(lexer.next_token(), Token::Number("0.5".to_string()));
    }

    #[test]
    fn test_quoted_identifiers() {
        let mut lexer = Lexer::new(r#""my table" "column\"name""#.to_string());
        assert_eq!(
            lexer.next_token(),
            Token::Identifier("my table".to_string())
        );
        assert_eq!(
            lexer.next_token(),
            Token::Identifier("column\"name".to_string())
        );
    }

    #[test]
    fn test_comments() {
        let mut lexer = Lexer::new("SELECT -- comment\n* FROM table".to_string());
        assert_eq!(lexer.next_token(), Token::Select);
        assert_eq!(lexer.next_token(), Token::Star);
        assert_eq!(lexer.next_token(), Token::From);
        assert_eq!(lexer.next_token(), Token::Table);
    }

    #[test]
    fn test_full_query() {
        let mut lexer = Lexer::new("SELECT id, name FROM users WHERE age > 18 AND status = 'active' ORDER BY id DESC LIMIT 10".to_string());
        let tokens = lexer.tokenize();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Identifier("id".to_string()));
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Identifier("name".to_string()));
        assert_eq!(tokens[4], Token::From);
        assert_eq!(tokens[5], Token::Identifier("users".to_string()));
        assert_eq!(tokens[6], Token::Where);
        assert_eq!(tokens[7], Token::Identifier("age".to_string()));
        assert_eq!(tokens[8], Token::Greater);
        assert_eq!(tokens[9], Token::Number("18".to_string()));
        assert_eq!(tokens[10], Token::And);
        assert_eq!(tokens[11], Token::Identifier("status".to_string()));
        assert_eq!(tokens[12], Token::Equal);
        assert_eq!(tokens[13], Token::String("active".to_string()));
        assert_eq!(tokens[14], Token::Order);
        assert_eq!(tokens[15], Token::By);
        assert_eq!(tokens[16], Token::Identifier("id".to_string()));
        assert_eq!(tokens[17], Token::Desc);
        assert_eq!(tokens[18], Token::Limit);
        assert_eq!(tokens[19], Token::Number("10".to_string()));
        assert_eq!(tokens[20], Token::Eof);
    }
}
