// PostgreSQL wire protocol message types

use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    // Frontend messages (client to server)
    Startup,
    Query,
    Parse,
    Bind,
    Execute,
    Sync,
    Terminate,
    Close,
    Describe,
    Flush,
    CopyData,
    CopyDone,
    CopyFail,
    PasswordMessage,

    // Backend messages (server to client)
    Authentication,
    BackendKeyData,
    BindComplete,
    CloseComplete,
    CommandComplete,
    CopyInResponse,
    CopyOutResponse,
    DataRow,
    EmptyQueryResponse,
    ErrorResponse,
    NoData,
    NoticeResponse,
    NotificationResponse,
    ParameterDescription,
    ParameterStatus,
    ParseComplete,
    PortalSuspended,
    ReadyForQuery,
    RowDescription,
}

impl MessageType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'Q' => Some(Self::Query),
            b'P' => Some(Self::Parse),
            b'B' => Some(Self::Bind),
            b'E' => Some(Self::Execute),
            b'S' => Some(Self::Sync),
            b'X' => Some(Self::Terminate),
            b'C' => Some(Self::Close),
            b'D' => Some(Self::Describe),
            b'H' => Some(Self::Flush),
            b'd' => Some(Self::CopyData),
            b'c' => Some(Self::CopyDone),
            b'f' => Some(Self::CopyFail),
            b'p' => Some(Self::PasswordMessage),
            _ => None,
        }
    }

    pub fn to_byte(&self) -> Option<u8> {
        match self {
            // Backend messages
            Self::Authentication => Some(b'R'),
            Self::BackendKeyData => Some(b'K'),
            Self::BindComplete => Some(b'2'),
            Self::CloseComplete => Some(b'3'),
            Self::CommandComplete => Some(b'C'),
            Self::CopyInResponse => Some(b'G'),
            Self::CopyOutResponse => Some(b'H'),
            Self::DataRow => Some(b'D'),
            Self::EmptyQueryResponse => Some(b'I'),
            Self::ErrorResponse => Some(b'E'),
            Self::NoData => Some(b'n'),
            Self::NoticeResponse => Some(b'N'),
            Self::NotificationResponse => Some(b'A'),
            Self::ParameterDescription => Some(b't'),
            Self::ParameterStatus => Some(b'S'),
            Self::ParseComplete => Some(b'1'),
            Self::PortalSuspended => Some(b's'),
            Self::ReadyForQuery => Some(b'Z'),
            Self::RowDescription => Some(b'T'),
            _ => None, // Frontend messages don't need to_byte in server
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    // Frontend messages
    Startup {
        version: i32,
        parameters: HashMap<String, String>,
    },
    Query {
        query: String,
    },
    Parse {
        statement_name: String,
        query: String,
        parameter_types: Vec<i32>,
    },
    Bind {
        portal_name: String,
        statement_name: String,
        parameter_formats: Vec<i16>,
        parameter_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Execute {
        portal_name: String,
        max_rows: i32,
    },
    Sync,
    Terminate,
    Close {
        close_type: u8, // 'S' for statement, 'P' for portal
        name: String,
    },
    Describe {
        describe_type: u8, // 'S' for statement, 'P' for portal
        name: String,
    },
    Flush,
    PasswordMessage {
        password: String,
    },

    // Backend messages
    Authentication {
        auth_type: AuthenticationType,
    },
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    BindComplete,
    CloseComplete,
    CommandComplete {
        tag: String,
    },
    DataRow {
        values: Vec<Option<Vec<u8>>>,
    },
    EmptyQueryResponse,
    ErrorResponse {
        fields: HashMap<u8, String>,
    },
    NoData,
    NoticeResponse {
        fields: HashMap<u8, String>,
    },
    ParameterStatus {
        name: String,
        value: String,
    },
    ParseComplete,
    PortalSuspended,
    ReadyForQuery {
        status: TransactionStatus,
    },
    RowDescription {
        fields: Vec<FieldDescription>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthenticationType {
    Ok = 0,
    KerberosV5 = 2,
    CleartextPassword = 3,
    MD5Password = 5,
    SCMCredential = 6,
    GSS = 7,
    GSSContinue = 8,
    SSPI = 9,
    SASL = 10,
    SASLContinue = 11,
    SASLFinal = 12,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionStatus {
    Idle = b'I' as isize,
    InTransaction = b'T' as isize,
    Failed = b'E' as isize,
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr_num: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

impl Message {
    pub fn encode(&self, buf: &mut BytesMut) -> crate::network::Result<()> {
        match self {
            Message::Authentication { auth_type } => {
                buf.put_u8(b'R');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                buf.put_i32(*auth_type as i32);
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            Message::ParameterStatus { name, value } => {
                buf.put_u8(b'S');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                buf.put_slice(name.as_bytes());
                buf.put_u8(0);
                buf.put_slice(value.as_bytes());
                buf.put_u8(0);
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            Message::BackendKeyData {
                process_id,
                secret_key,
            } => {
                buf.put_u8(b'K');
                buf.put_i32(12); // fixed length
                buf.put_i32(*process_id);
                buf.put_i32(*secret_key);
            }
            Message::ReadyForQuery { status } => {
                buf.put_u8(b'Z');
                buf.put_i32(5); // fixed length
                buf.put_u8(*status as u8);
            }
            Message::RowDescription { fields } => {
                buf.put_u8(b'T');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                buf.put_i16(fields.len() as i16);
                for field in fields {
                    buf.put_slice(field.name.as_bytes());
                    buf.put_u8(0);
                    buf.put_i32(field.table_oid);
                    buf.put_i16(field.column_attr_num);
                    buf.put_i32(field.type_oid);
                    buf.put_i16(field.type_size);
                    buf.put_i32(field.type_modifier);
                    buf.put_i16(field.format_code);
                }
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            Message::DataRow { values } => {
                buf.put_u8(b'D');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                buf.put_i16(values.len() as i16);
                for value in values {
                    if let Some(data) = value {
                        buf.put_i32(data.len() as i32);
                        buf.put_slice(data);
                    } else {
                        buf.put_i32(-1); // NULL
                    }
                }
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            Message::CommandComplete { tag } => {
                buf.put_u8(b'C');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                buf.put_slice(tag.as_bytes());
                buf.put_u8(0);
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            Message::EmptyQueryResponse => {
                buf.put_u8(b'I');
                buf.put_i32(4); // fixed length
            }
            Message::ParseComplete => {
                buf.put_u8(b'1');
                buf.put_i32(4); // fixed length
            }
            Message::BindComplete => {
                buf.put_u8(b'2');
                buf.put_i32(4); // fixed length
            }
            Message::CloseComplete => {
                buf.put_u8(b'3');
                buf.put_i32(4); // fixed length
            }
            Message::NoData => {
                buf.put_u8(b'n');
                buf.put_i32(4); // fixed length
            }
            Message::PortalSuspended => {
                buf.put_u8(b's');
                buf.put_i32(4); // fixed length
            }
            Message::ErrorResponse { fields } => {
                buf.put_u8(b'E');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                for (field_type, value) in fields {
                    buf.put_u8(*field_type);
                    buf.put_slice(value.as_bytes());
                    buf.put_u8(0);
                }
                buf.put_u8(0); // terminator
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            Message::NoticeResponse { fields } => {
                buf.put_u8(b'N');
                let start = buf.len();
                buf.put_i32(0); // placeholder for length
                for (field_type, value) in fields {
                    buf.put_u8(*field_type);
                    buf.put_slice(value.as_bytes());
                    buf.put_u8(0);
                }
                buf.put_u8(0); // terminator
                let len = (buf.len() - start) as i32;
                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
            }
            _ => {
                return Err(crate::network::NetworkError::UnsupportedFeature(format!(
                    "Encoding for message type {:?} not implemented",
                    self
                )))
            }
        }
        Ok(())
    }

    pub fn decode_startup(buf: &mut BytesMut) -> crate::network::Result<Option<Self>> {
        if buf.remaining() < 4 {
            return Ok(None);
        }

        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < len {
            return Ok(None);
        }

        buf.advance(4);
        let version = buf.get_i32();
        let mut parameters = HashMap::new();

        while buf.remaining() > 0 {
            let name = read_cstring(buf)?;
            if name.is_empty() {
                break;
            }
            let value = read_cstring(buf)?;
            parameters.insert(name, value);
        }

        Ok(Some(Message::Startup {
            version,
            parameters,
        }))
    }

    pub fn decode(msg_type: MessageType, buf: &mut BytesMut) -> crate::network::Result<Self> {
        match msg_type {
            MessageType::Query => {
                let query = read_cstring(buf)?;
                Ok(Message::Query { query })
            }
            MessageType::Parse => {
                let statement_name = read_cstring(buf)?;
                let query = read_cstring(buf)?;
                let param_count = buf.get_i16() as usize;
                let mut parameter_types = Vec::with_capacity(param_count);
                for _ in 0..param_count {
                    parameter_types.push(buf.get_i32());
                }
                Ok(Message::Parse {
                    statement_name,
                    query,
                    parameter_types,
                })
            }
            MessageType::Bind => {
                let portal_name = read_cstring(buf)?;
                let statement_name = read_cstring(buf)?;
                let format_count = buf.get_i16() as usize;
                let mut parameter_formats = Vec::with_capacity(format_count);
                for _ in 0..format_count {
                    parameter_formats.push(buf.get_i16());
                }
                let param_count = buf.get_i16() as usize;
                let mut parameter_values = Vec::with_capacity(param_count);
                for _ in 0..param_count {
                    let len = buf.get_i32();
                    if len == -1 {
                        parameter_values.push(None);
                    } else {
                        let mut data = vec![0u8; len as usize];
                        buf.copy_to_slice(&mut data);
                        parameter_values.push(Some(data));
                    }
                }
                let result_format_count = buf.get_i16() as usize;
                let mut result_formats = Vec::with_capacity(result_format_count);
                for _ in 0..result_format_count {
                    result_formats.push(buf.get_i16());
                }
                Ok(Message::Bind {
                    portal_name,
                    statement_name,
                    parameter_formats,
                    parameter_values,
                    result_formats,
                })
            }
            MessageType::Execute => {
                let portal_name = read_cstring(buf)?;
                let max_rows = buf.get_i32();
                Ok(Message::Execute {
                    portal_name,
                    max_rows,
                })
            }
            MessageType::Describe => {
                let describe_type = buf.get_u8();
                let name = read_cstring(buf)?;
                Ok(Message::Describe {
                    describe_type,
                    name,
                })
            }
            MessageType::Close => {
                let close_type = buf.get_u8();
                let name = read_cstring(buf)?;
                Ok(Message::Close { close_type, name })
            }
            MessageType::Sync => Ok(Message::Sync),
            MessageType::Flush => Ok(Message::Flush),
            MessageType::Terminate => Ok(Message::Terminate),
            MessageType::PasswordMessage => {
                let password = read_cstring(buf)?;
                Ok(Message::PasswordMessage { password })
            }
            _ => Err(crate::network::NetworkError::InvalidMessage(format!(
                "Unsupported message type: {:?}",
                msg_type
            ))),
        }
    }
}

fn read_cstring(buf: &mut BytesMut) -> crate::network::Result<String> {
    let pos = buf.iter().position(|&b| b == 0).ok_or_else(|| {
        crate::network::NetworkError::InvalidMessage("Missing null terminator".to_string())
    })?;
    let s = std::str::from_utf8(&buf[..pos])
        .map_err(|e| crate::network::NetworkError::InvalidMessage(format!("Invalid UTF-8: {}", e)))?
        .to_string();
    buf.advance(pos + 1);
    Ok(s)
}

// PostgreSQL type OIDs
pub mod type_oids {
    pub const BOOL: i32 = 16;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const INT8: i32 = 20;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const TEXT: i32 = 25;
    pub const VARCHAR: i32 = 1043;
    pub const TIMESTAMP: i32 = 1114;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_encoding() {
        let mut buf = BytesMut::new();
        let msg = Message::ReadyForQuery {
            status: TransactionStatus::Idle,
        };
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'Z');
        assert_eq!(buf[1..5], [0, 0, 0, 5]); // length
        assert_eq!(buf[5], b'I'); // status
    }

    #[test]
    fn test_startup_message_decoding() {
        let mut buf = BytesMut::new();
        buf.put_i32(29); // length
        buf.put_i32(196608); // version 3.0
        buf.put_slice(b"user\0");
        buf.put_slice(b"postgres\0");
        buf.put_slice(b"database\0");
        buf.put_slice(b"test\0");
        buf.put_u8(0); // terminator

        let msg = Message::decode_startup(&mut buf).unwrap().unwrap();
        match msg {
            Message::Startup {
                version,
                parameters,
            } => {
                assert_eq!(version, 196608);
                assert_eq!(parameters.get("user"), Some(&"postgres".to_string()));
                assert_eq!(parameters.get("database"), Some(&"test".to_string()));
            }
            _ => panic!("Expected Startup message"),
        }
    }
}
