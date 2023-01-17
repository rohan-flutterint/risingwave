// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::io::{self, Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::pin::Pin;
use std::str::Utf8Error;
use std::sync::Arc;
use std::{str, vec};

use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use futures::Stream;
use openssl::ssl::{SslAcceptor, SslContext, SslContextRef, SslMethod};
use risingwave_sqlparser::parser::Parser;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_openssl::SslStream;
use tracing::log::trace;
use tracing::warn;

use crate::error::{PsqlError, PsqlResult};
use crate::pg_extended::{PgPortal, PgStatement, PreparedStatement};
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeBindMessage, FeCancelMessage,
    FeCloseMessage, FeDescribeMessage, FeExecuteMessage, FeMessage, FeParseMessage,
    FePasswordMessage, FeStartupMessage,
};
use crate::pg_response::RowSetResult;
use crate::pg_server::{Session, SessionManager, UserAuthenticator};

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol<S, SM, VS>
where
    SM: SessionManager<VS>,
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    /// Used for write/read pg messages.
    stream: Conn<S>,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<SM>,
    session: Option<Arc<SM::Session>>,

    unnamed_statement: Option<PgStatement>,
    unnamed_portal: Option<PgPortal<VS>>,
    named_statements: HashMap<String, PgStatement>,
    named_portals: HashMap<String, PgPortal<VS>>,

    // Used for ssl connection.
    // If None, not expected to build ssl connection (panic).
    tls_context: Option<SslContext>,
}

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// The path to the TLS certificate.
    pub cert: PathBuf,
    /// The path to the TLS key.
    pub key: PathBuf,
}

impl TlsConfig {
    pub fn new_default() -> Self {
        let cert = PathBuf::new().join("tests/ssl/demo.crt");
        let key = PathBuf::new().join("tests/ssl/demo.key");
        let path_to_cur_proj = PathBuf::new().join("src/utils/pgwire");

        Self {
            // Now the demo crt and key are hard code generated via simple self-signed CA.
            // In future it should change to configure by user.
            // The path is mounted from project root.
            cert: path_to_cur_proj.join(cert),
            key: path_to_cur_proj.join(key),
        }
    }
}

impl<S, SM, VS> Drop for PgProtocol<S, SM, VS>
where
    SM: SessionManager<VS>,
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            // Clear the session in session manager.
            self.session_mgr.end_session(session);
        }
    }
}

/// States flow happened from top to down.
enum PgProtocolState {
    Startup,
    Regular,
}

/// Truncate 0 from C string in Bytes and stringify it (returns slice, no allocations).
///
/// PG protocol strings are always C strings.
pub fn cstr_to_str(b: &Bytes) -> Result<&str, Utf8Error> {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null)
}

impl<S, SM, VS> PgProtocol<S, SM, VS>
where
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager<VS>,
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    pub fn new(stream: S, session_mgr: Arc<SM>, tls_config: Option<TlsConfig>) -> Self {
        Self {
            stream: Conn::Unencrypted(PgStream {
                stream: Some(stream),
                write_buf: BytesMut::with_capacity(10 * 1024),
            }),
            is_terminate: false,
            state: PgProtocolState::Startup,
            session_mgr,
            session: None,
            unnamed_statement: None,
            unnamed_portal: None,
            named_statements: Default::default(),
            named_portals: Default::default(),
            tls_context: tls_config
                .as_ref()
                .and_then(|e| build_ssl_ctx_from_config(e).ok()),
        }
    }

    /// Processes one message. Returns true if the connection is terminated.
    pub async fn process(&mut self, msg: FeMessage) -> bool {
        self.do_process(msg).await || self.is_terminate
    }

    async fn do_process(&mut self, msg: FeMessage) -> bool {
        match self.do_process_inner(msg).await {
            Ok(v) => v,
            Err(e) => {
                match e {
                    PsqlError::IoError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return true;
                        }
                    }

                    PsqlError::StartupError(_)
                    | PsqlError::PasswordError(_)
                    | PsqlError::SslError(_) => {
                        // TODO: Fix the unwrap in this stream.
                        self.stream
                            .write_no_flush(&BeMessage::ErrorResponse(Box::new(e)))
                            .unwrap();
                        self.stream.flush().await.unwrap_or_else(|e| {
                            tracing::error!("flush error: {}", e);
                        });
                        return true;
                    }

                    PsqlError::QueryError(_) => {
                        self.stream
                            .write_no_flush(&BeMessage::ErrorResponse(Box::new(e)))
                            .unwrap();
                        self.stream
                            .write_no_flush(&BeMessage::ReadyForQuery)
                            .unwrap();
                    }

                    PsqlError::Internal(_)
                    | PsqlError::ParseError(_)
                    | PsqlError::ExecuteError(_) => {
                        self.stream
                            .write_no_flush(&BeMessage::ErrorResponse(Box::new(e)))
                            .unwrap();
                    }
                }
                self.stream.flush().await.unwrap_or_else(|e| {
                    tracing::error!("flush error: {}", e);
                });
                false
            }
        }
    }

    async fn do_process_inner(&mut self, msg: FeMessage) -> PsqlResult<bool> {
        match msg {
            FeMessage::Ssl => self.process_ssl_msg().await?,
            FeMessage::Startup(msg) => self.process_startup_msg(msg)?,
            FeMessage::Password(msg) => self.process_password_msg(msg)?,
            FeMessage::Query(query_msg) => self.process_query_msg(query_msg.get_sql()).await?,
            FeMessage::CancelQuery(m) => self.process_cancel_msg(m)?,
            FeMessage::Terminate => self.process_terminate(),
            FeMessage::Parse(m) => self.process_parse_msg(m).await?,
            FeMessage::Bind(m) => self.process_bind_msg(m)?,
            FeMessage::Execute(m) => self.process_execute_msg(m).await?,
            FeMessage::Describe(m) => self.process_describe_msg(m)?,
            FeMessage::Sync => self.stream.write_no_flush(&BeMessage::ReadyForQuery)?,
            FeMessage::Close(m) => self.process_close_msg(m)?,
            FeMessage::Flush => self.stream.flush().await?,
        }
        self.stream.flush().await?;
        Ok(false)
    }

    pub async fn read_message(&mut self) -> io::Result<FeMessage> {
        match self.state {
            PgProtocolState::Startup => self.stream.read_startup().await,
            PgProtocolState::Regular => self.stream.read().await,
        }
    }

    async fn process_ssl_msg(&mut self) -> PsqlResult<()> {
        if let Some(context) = self.tls_context.as_ref() {
            // If got and ssl context, say yes for ssl connection.
            // Construct ssl stream and replace with current one.
            self.stream.write(&BeMessage::EncryptionResponseYes).await?;
            let ssl_stream = self.stream.ssl(context).await?;
            self.stream = Conn::Ssl(ssl_stream);
        } else {
            // If no, say no for encryption.
            self.stream.write(&BeMessage::EncryptionResponseNo).await?;
        }

        Ok(())
    }

    fn process_startup_msg(&mut self, msg: FeStartupMessage) -> PsqlResult<()> {
        let db_name = msg
            .config
            .get("database")
            .cloned()
            .unwrap_or_else(|| "dev".to_string());
        let user_name = msg
            .config
            .get("user")
            .cloned()
            .unwrap_or_else(|| "root".to_string());

        let session = self
            .session_mgr
            .connect(&db_name, &user_name)
            .map_err(PsqlError::StartupError)?;
        match session.user_authenticator() {
            UserAuthenticator::None => {
                self.stream.write_no_flush(&BeMessage::AuthenticationOk)?;

                // Cancel request need this for identify and verification. According to postgres
                // doc, it should be written to buffer after receive AuthenticationOk.
                // let id = self.session_mgr.insert_session(session.clone());
                self.stream
                    .write_no_flush(&BeMessage::BackendKeyData(session.id()))?;

                self.stream.write_parameter_status_msg_no_flush()?;
                self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
            }
            UserAuthenticator::ClearText(_) => {
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationCleartextPassword)?;
            }
            UserAuthenticator::Md5WithSalt { salt, .. } => {
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationMd5Password(salt))?;
            }
        }
        self.session = Some(session);
        self.state = PgProtocolState::Regular;
        Ok(())
    }

    fn process_password_msg(&mut self, msg: FePasswordMessage) -> PsqlResult<()> {
        let authenticator = self.session.as_ref().unwrap().user_authenticator();
        if !authenticator.authenticate(&msg.password) {
            return Err(PsqlError::PasswordError(IoError::new(
                ErrorKind::InvalidInput,
                "Invalid password",
            )));
        }
        self.stream.write_no_flush(&BeMessage::AuthenticationOk)?;
        self.stream.write_parameter_status_msg_no_flush()?;
        self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
        self.state = PgProtocolState::Regular;
        Ok(())
    }

    fn process_cancel_msg(&mut self, m: FeCancelMessage) -> PsqlResult<()> {
        let session_id = (m.target_process_id, m.target_secret_key);
        self.session_mgr.cancel_queries_in_session(session_id);
        self.stream.write_no_flush(&BeMessage::EmptyQueryResponse)?;
        Ok(())
    }

    async fn process_query_msg(&mut self, query_string: io::Result<&str>) -> PsqlResult<()> {
        let sql = query_string.map_err(|err| PsqlError::QueryError(Box::new(err)))?;
        tracing::trace!(
            target: "pgwire_query_log",
            "(simple query)receive query: {}", sql);

        let session = self.session.clone().unwrap();

        // Parse sql.
        let stmts = Parser::parse_sql(sql)
            .inspect_err(|e| tracing::error!("failed to parse sql:\n{}:\n{}", sql, e))
            .map_err(|err| PsqlError::QueryError(err.into()))?;

        // Execute multiple statements in simple query. KISS later.
        for stmt in stmts {
            let session = session.clone();

            // execute query
            let mut res = session
                .run_one_query(stmt, false)
                .await
                .map_err(|err| PsqlError::QueryError(err))?;

            if let Some(notice) = res.get_notice() {
                self.stream
                    .write_no_flush(&BeMessage::NoticeResponse(&notice))?;
            }

            if res.is_query() {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&res.get_row_desc()))?;

                let mut rows_cnt = 0;

                while let Some(row_set) = res.values_stream().next().await {
                    let row_set = row_set.map_err(|err| PsqlError::QueryError(err))?;
                    for row in row_set {
                        self.stream.write_no_flush(&BeMessage::DataRow(&row))?;
                        rows_cnt += 1;
                    }
                }

                self.stream.write_no_flush(&BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: res.get_stmt_type(),
                        rows_cnt,
                    },
                ))?;
            } else {
                self.stream.write_no_flush(&BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: res.get_stmt_type(),
                        rows_cnt: res
                            .get_effected_rows_cnt()
                            .expect("row count should be set"),
                    },
                ))?;
            }
            self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
        }
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_parse_msg(&mut self, msg: FeParseMessage) -> PsqlResult<()> {
        let sql = cstr_to_str(&msg.sql_bytes).unwrap();
        tracing::trace!("(extended query)parse query: {}", sql);

        // Flag indicate whether statement is a query statement.
        let is_query_sql = {
            let lower_sql = sql.to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
                || lower_sql.starts_with("explain")
        };

        let prepared_statement = PreparedStatement::parse_statement(sql.to_string(), msg.type_ids)?;

        // 2. Create the row description.
        let fields: Vec<PgFieldDescriptor> = if is_query_sql {
            let sql = prepared_statement.instance_default()?;

            let session = self.session.clone().unwrap();
            session
                .infer_return_type(&sql)
                .await
                .map_err(PsqlError::ParseError)?
        } else {
            vec![]
        };

        // 3. Create the statement.
        let statement = PgStatement::new(
            cstr_to_str(&msg.statement_name).unwrap().to_string(),
            prepared_statement,
            fields,
            is_query_sql,
        );

        // 4. Insert the statement.
        let name = statement.name();
        if name.is_empty() {
            self.unnamed_statement.replace(statement);
        } else {
            self.named_statements.insert(name, statement);
        }
        self.stream.write_no_flush(&BeMessage::ParseComplete)?;
        Ok(())
    }

    fn process_bind_msg(&mut self, msg: FeBindMessage) -> PsqlResult<()> {
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_string();
        // 1. Get statement.
        trace!(
            target: "pgwire_query_log",
            "(extended query)bind: get statement name: {}",
            &statement_name
        );
        let statement = if statement_name.is_empty() {
            self.unnamed_statement
                .as_ref()
                .ok_or_else(PsqlError::no_statement)?
        } else {
            self.named_statements
                .get(&statement_name)
                .ok_or_else(PsqlError::no_statement)?
        };

        // 2. Instance the statement to get the portal.
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let portal = statement.instance(
            portal_name.clone(),
            &msg.params,
            msg.result_format_code,
            msg.param_format_code,
        )?;

        // 3. Insert the Portal.
        if portal_name.is_empty() {
            self.unnamed_portal.replace(portal);
        } else {
            self.named_portals.insert(portal_name, portal);
        }
        self.stream.write_no_flush(&BeMessage::BindComplete)?;
        Ok(())
    }

    async fn process_execute_msg(&mut self, msg: FeExecuteMessage) -> PsqlResult<()> {
        // 1. Get portal.
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let portal = if msg.portal_name.is_empty() {
            self.unnamed_portal
                .as_mut()
                .ok_or_else(PsqlError::no_portal)?
        } else {
            // NOTE Error handle need modify later.
            self.named_portals
                .get_mut(&portal_name)
                .ok_or_else(PsqlError::no_portal)?
        };

        tracing::trace!(target: "pgwire_query_log", "(extended query)execute query: {}", portal.query_string());

        // 2. Execute instance statement using portal.
        let session = self.session.clone().unwrap();
        portal
            .execute::<SM, S>(session, msg.max_rows.try_into().unwrap(), &mut self.stream)
            .await?;

        // NOTE there is no ReadyForQuery message.
        Ok(())
    }

    fn process_describe_msg(&mut self, msg: FeDescribeMessage) -> PsqlResult<()> {
        //  b'S' => Statement
        //  b'P' => Portal
        tracing::trace!(
            target: "pgwire_query_log",
            "(extended query)describe name: {}",
            cstr_to_str(&msg.name).unwrap()
        );

        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            let name = cstr_to_str(&msg.name).unwrap().to_string();
            let statement = if name.is_empty() {
                self.unnamed_statement
                    .as_ref()
                    .ok_or_else(PsqlError::no_statement)?
            } else {
                // NOTE Error handle need modify later.
                self.named_statements
                    .get(&name)
                    .ok_or_else(PsqlError::no_statement)?
            };

            // 1. Send parameter description.
            self.stream
                .write_no_flush(&BeMessage::ParameterDescription(
                    &statement.param_oid_desc(),
                ))?;

            // 2. Send row description.
            if statement.is_query() {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&statement.row_desc()))?;
            } else {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(&BeMessage::NoData)?;
            }
        } else if msg.kind == b'P' {
            let name = cstr_to_str(&msg.name).unwrap().to_string();
            let portal = if name.is_empty() {
                self.unnamed_portal
                    .as_ref()
                    .ok_or_else(PsqlError::no_portal)?
            } else {
                // NOTE Error handle need modify later.
                self.named_portals
                    .get(&name)
                    .ok_or_else(PsqlError::no_portal)?
            };

            // 3. Send row description.
            if portal.is_query() {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&portal.row_desc()))?;
            } else {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(&BeMessage::NoData)?;
            }
        }
        Ok(())
    }

    fn process_close_msg(&mut self, msg: FeCloseMessage) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.name).unwrap().to_string();
        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            self.named_statements.remove_entry(&name);
        } else if msg.kind == b'P' {
            self.named_portals.remove_entry(&name);
        }
        self.stream.write_no_flush(&BeMessage::CloseComplete)?;
        Ok(())
    }
}

/// Wraps a byte stream and read/write pg messages.
pub struct PgStream<S> {
    /// The underlying stream.
    stream: Option<S>,
    /// Write into buffer before flush to stream.
    write_buf: BytesMut,
}

impl<S> PgStream<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn read_startup(&mut self) -> io::Result<FeMessage> {
        FeStartupMessage::read(self.stream()).await
    }

    async fn read(&mut self) -> io::Result<FeMessage> {
        FeMessage::read(self.stream()).await
    }

    fn write_parameter_status_msg_no_flush(&mut self) -> io::Result<()> {
        self.write_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ClientEncoding("UTF8"),
        ))?;
        self.write_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString("on"),
        ))?;
        self.write_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion("9.5.0"),
        ))?;
        Ok(())
    }

    pub fn write_no_flush(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        BeMessage::write(&mut self.write_buf, message)
    }

    async fn write(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        self.write_no_flush(message)?;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&self.write_buf)
            .await?;
        self.write_buf.clear();
        self.stream.as_mut().unwrap().flush().await?;
        Ok(())
    }

    fn stream(&mut self) -> &mut (impl AsyncRead + Unpin + AsyncWrite) {
        self.stream.as_mut().unwrap()
    }
}

/// The logic of Conn is very simple, just a static dispatcher for TcpStream: Unencrypted or Ssl:
/// Encrypted.
pub enum Conn<S> {
    Unencrypted(PgStream<S>),
    Ssl(PgStream<SslStream<S>>),
}

impl<S> PgStream<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn ssl(&mut self, ssl_ctx: &SslContextRef) -> PsqlResult<PgStream<SslStream<S>>> {
        // Note: Currently we take the ownership of previous Tcp Stream and then turn into a
        // SslStream. Later we can avoid storing stream inside PgProtocol to do this more
        // fluently.
        let stream = self.stream.take().unwrap();
        let ssl = openssl::ssl::Ssl::new(ssl_ctx).unwrap();
        let mut stream = tokio_openssl::SslStream::new(ssl, stream).unwrap();
        if let Err(e) = Pin::new(&mut stream).accept().await {
            warn!("Unable to set up a ssl connection, reason: {}", e);
            let _ = stream.shutdown().await;
            return Err(PsqlError::SslError(e.to_string()));
        }

        Ok(PgStream {
            stream: Some(stream),
            write_buf: BytesMut::with_capacity(10 * 1024),
        })
    }
}

impl<S> Conn<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn read_startup(&mut self) -> io::Result<FeMessage> {
        match self {
            Conn::Unencrypted(s) => s.read_startup().await,
            Conn::Ssl(s) => s.read_startup().await,
        }
    }

    async fn read(&mut self) -> io::Result<FeMessage> {
        match self {
            Conn::Unencrypted(s) => s.read().await,
            Conn::Ssl(s) => s.read().await,
        }
    }

    fn write_parameter_status_msg_no_flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.write_parameter_status_msg_no_flush(),
            Conn::Ssl(s) => s.write_parameter_status_msg_no_flush(),
        }
    }

    pub fn write_no_flush(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.write_no_flush(message),
            Conn::Ssl(s) => s.write_no_flush(message),
        }
        .map_err(|e| {
            tracing::error!("flush error: {}", e);
            e
        })
    }

    async fn write(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.write(message).await,
            Conn::Ssl(s) => s.write(message).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.flush().await,
            Conn::Ssl(s) => s.flush().await,
        }
    }

    async fn ssl(&mut self, ssl_ctx: &SslContextRef) -> PsqlResult<PgStream<SslStream<S>>> {
        match self {
            Conn::Unencrypted(s) => s.ssl(ssl_ctx).await,
            Conn::Ssl(_s) => panic!("can not turn a ssl stream into a ssl stream"),
        }
    }
}

fn build_ssl_ctx_from_config(tls_config: &TlsConfig) -> PsqlResult<SslContext> {
    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();

    let key_path = &tls_config.key;
    let cert_path = &tls_config.cert;

    // Build ssl acceptor according to the config.
    // Now we set every verify to true.
    acceptor
        .set_private_key_file(key_path, openssl::ssl::SslFiletype::PEM)
        .map_err(|e| PsqlError::Internal(e.into()))?;
    acceptor
        .set_ca_file(cert_path)
        .map_err(|e| PsqlError::Internal(e.into()))?;
    acceptor
        .set_certificate_chain_file(cert_path)
        .map_err(|e| PsqlError::Internal(e.into()))?;
    let acceptor = acceptor.build();

    Ok(acceptor.into_context())
}
