// PostgreSQL wire protocol handler

use crate::{
    access::{DataType, Value, deserialize_values},
    database::Database,
    session::{Session, QueryResult},
    transaction::{id::TransactionId, manager::TransactionManager},
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use super::message::{
    type_oids, AuthenticationType, FieldDescription, Message, MessageType, TransactionStatus,
};

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Invalid protocol version: {0}")]
    InvalidVersion(i32),

    #[error("Missing required parameter: {0}")]
    MissingParameter(String),

    #[error("Invalid message format")]
    InvalidMessageFormat,

    #[error("Unexpected message type: {0:?}")]
    UnexpectedMessage(MessageType),

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),
}

pub struct ProtocolHandler {
    database: Arc<Database>,
    transaction_manager: Arc<TransactionManager>,
    session: Session,
    current_transaction_id: Option<TransactionId>,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
}

struct PreparedStatement {
    query: String,
    _parameter_types: Vec<i32>,
}

#[derive(Clone)]
struct Portal {
    statement_name: String,
    _parameter_values: Vec<Option<Vec<u8>>>,
    _result_formats: Vec<i16>,
}

impl ProtocolHandler {
    pub fn new(database: Arc<Database>, transaction_manager: Arc<TransactionManager>) -> Self {
        let session = Session::new(database.clone());
        Self {
            database,
            transaction_manager,
            session,
            current_transaction_id: None,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    pub fn handle_startup(
        &mut self,
        version: i32,
        parameters: HashMap<String, String>,
    ) -> Result<Vec<Message>, ProtocolError> {
        if version != crate::network::PROTOCOL_VERSION_3 {
            return Err(ProtocolError::InvalidVersion(version));
        }

        let responses = vec![
            // Send authentication OK
            Message::Authentication {
                auth_type: AuthenticationType::Ok,
            },
            // Send parameter status messages
            Message::ParameterStatus {
                name: "server_version".to_string(),
                value: "15.0".to_string(),
            },
            Message::ParameterStatus {
                name: "server_encoding".to_string(),
                value: "UTF8".to_string(),
            },
            Message::ParameterStatus {
                name: "client_encoding".to_string(),
                value: "UTF8".to_string(),
            },
            Message::ParameterStatus {
                name: "DateStyle".to_string(),
                value: "ISO, MDY".to_string(),
            },
            Message::ParameterStatus {
                name: "integer_datetimes".to_string(),
                value: "on".to_string(),
            },
            Message::ParameterStatus {
                name: "IntervalStyle".to_string(),
                value: "postgres".to_string(),
            },
            Message::ParameterStatus {
                name: "is_superuser".to_string(),
                value: "off".to_string(),
            },
            Message::ParameterStatus {
                name: "session_authorization".to_string(),
                value: parameters
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "vibedb".to_string()),
            },
            Message::ParameterStatus {
                name: "standard_conforming_strings".to_string(),
                value: "on".to_string(),
            },
            Message::ParameterStatus {
                name: "TimeZone".to_string(),
                value: "UTC".to_string(),
            },
            // Backend key data
            Message::BackendKeyData {
                process_id: std::process::id() as i32,
                secret_key: rand::random::<u32>() as i32,
            },
            // Ready for query
            Message::ReadyForQuery {
                status: TransactionStatus::Idle,
            },
        ];

        Ok(responses)
    }

    pub fn handle_message(&mut self, msg: Message) -> Result<Vec<Message>, ProtocolError> {
        match msg {
            Message::Query { query } => self.handle_query(&query),
            Message::Parse {
                statement_name,
                query,
                parameter_types,
            } => self.handle_parse(statement_name, query, parameter_types),
            Message::Bind {
                portal_name,
                statement_name,
                parameter_formats: _,
                parameter_values,
                result_formats,
            } => self.handle_bind(portal_name, statement_name, parameter_values, result_formats),
            Message::Execute {
                portal_name,
                max_rows: _,
            } => self.handle_execute(&portal_name),
            Message::Sync => Ok(vec![Message::ReadyForQuery {
                status: self.get_transaction_status(),
            }]),
            Message::Terminate => Ok(vec![]),
            _ => {
                let msg_type = match &msg {
                    Message::Query { .. } => MessageType::Query,
                    Message::Parse { .. } => MessageType::Parse,
                    Message::Bind { .. } => MessageType::Bind,
                    Message::Execute { .. } => MessageType::Execute,
                    Message::Sync => MessageType::Sync,
                    Message::Terminate => MessageType::Terminate,
                    _ => MessageType::Query, // Default
                };
                Err(ProtocolError::UnexpectedMessage(msg_type))
            }
        }
    }

    fn handle_query(&mut self, query: &str) -> Result<Vec<Message>, ProtocolError> {
        let mut responses = Vec::new();

        // Execute query using session
        match self.session.execute_sql(query) {
            Ok(result) => match result {
                QueryResult::Select { schema, rows } => {
                    eprintln!("DEBUG: Handling SELECT result with {} rows", rows.len());
                    // Send row description
                    let fields: Vec<FieldDescription> = schema
                        .iter()
                        .enumerate()
                        .map(|(i, col)| FieldDescription {
                            name: col.name.clone(),
                            table_oid: 0,
                            column_attr_num: (i + 1) as i16,
                            type_oid: data_type_to_oid(&col.data_type),
                            type_size: data_type_size(&col.data_type),
                            type_modifier: -1,
                            format_code: 0, // text format
                        })
                        .collect();
                    eprintln!("DEBUG: Sending RowDescription with {} fields", fields.len());
                    responses.push(Message::RowDescription { fields });

                    // Send rows
                    let row_count = rows.len();
                    for (i, row) in rows.iter().enumerate() {
                        eprintln!("DEBUG: Processing row {}/{}", i + 1, row_count);
                        // Get column types from schema
                        let data_types: Vec<DataType> = schema.iter().map(|col| col.data_type).collect();
                        
                        // Deserialize row data
                        let values = deserialize_values(&row.data, &data_types)
                            .map_err(|e| ProtocolError::ExecutionError(e.to_string()))?;
                        
                        // Convert to text format
                        let row_values: Vec<Option<Vec<u8>>> = values.iter()
                            .map(|v| value_to_text(v).map(|s| s.into_bytes()))
                            .collect();
                        eprintln!("DEBUG: Sending DataRow with {} values", row_values.len());
                        responses.push(Message::DataRow { values: row_values });
                    }

                    eprintln!("DEBUG: Sending CommandComplete for {} rows", row_count);
                    responses.push(Message::CommandComplete {
                        tag: format!("SELECT {}", row_count),
                    });
                }
                QueryResult::Insert { row_count, .. } => {
                    responses.push(Message::CommandComplete {
                        tag: format!("INSERT 0 {}", row_count),
                    });
                }
                QueryResult::CreateTable { table_name } => {
                    responses.push(Message::CommandComplete {
                        tag: format!("CREATE TABLE {}", table_name),
                    });
                }
            },
            Err(e) => {
                responses.push(create_error_response(
                    "42000",
                    &format!("Query execution failed: {}", e),
                ));
            }
        }

        responses.push(Message::ReadyForQuery {
            status: self.get_transaction_status(),
        });

        Ok(responses)
    }

    fn handle_parse(
        &mut self,
        statement_name: String,
        query: String,
        parameter_types: Vec<i32>,
    ) -> Result<Vec<Message>, ProtocolError> {
        // Store prepared statement
        self.prepared_statements.insert(
            statement_name,
            PreparedStatement {
                query,
                _parameter_types: parameter_types,
            },
        );
        Ok(vec![Message::ParseComplete])
    }

    fn handle_bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        parameter_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<Vec<Message>, ProtocolError> {
        // Store portal
        self.portals.insert(
            portal_name,
            Portal {
                statement_name,
                _parameter_values: parameter_values,
                _result_formats: result_formats,
            },
        );
        Ok(vec![Message::BindComplete])
    }

    fn handle_execute(&mut self, portal_name: &str) -> Result<Vec<Message>, ProtocolError> {
        let portal = self
            .portals
            .get(portal_name)
            .ok_or_else(|| ProtocolError::MissingParameter(format!("Portal {}", portal_name)))?
            .clone();

        let query = self
            .prepared_statements
            .get(&portal.statement_name)
            .ok_or_else(|| {
                ProtocolError::MissingParameter(format!("Statement {}", portal.statement_name))
            })?
            .query
            .clone();

        // Execute the prepared statement's query
        self.handle_query(&query)
    }

    fn get_transaction_status(&self) -> TransactionStatus {
        if self.current_transaction_id.is_some() {
            TransactionStatus::InTransaction
        } else {
            TransactionStatus::Idle
        }
    }
}

/// Convert Value to text representation for wire protocol
fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Boolean(b) => Some(if *b { "t".to_string() } else { "f".to_string() }),
        Value::Int32(i) => Some(i.to_string()),
        Value::String(s) => Some(s.clone()),
    }
}

/// Create error response message
fn create_error_response(code: &str, message: &str) -> Message {
    let mut fields = HashMap::new();
    fields.insert('S' as u8, "ERROR".to_string());
    fields.insert('C' as u8, code.to_string());
    fields.insert('M' as u8, message.to_string());
    Message::ErrorResponse { fields }
}

/// Get PostgreSQL type OID for our DataType
fn data_type_to_oid(data_type: &DataType) -> i32 {
    match data_type {
        DataType::Boolean => type_oids::BOOL,
        DataType::Int32 => type_oids::INT4,
        DataType::Varchar => type_oids::VARCHAR,
    }
}

/// Get type size for our DataType  
fn data_type_size(data_type: &DataType) -> i16 {
    match data_type {
        DataType::Boolean => 1,
        DataType::Int32 => 4,
        DataType::Varchar => -1, // variable length
    }
}

// Re-export rand for backend key generation
use rand;