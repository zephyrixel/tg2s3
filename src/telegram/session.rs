use grammers_session::types::{
    ChannelKind, ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerKind, UpdateState,
    UpdatesState,
};
use grammers_session::{BoxFuture, Session, SessionData};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::fmt;
use std::net::{AddrParseError, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

const PEER_USER_SELF: i64 = 1;
const PEER_USER_BOT: i64 = 2;
const PEER_MEGAGROUP: i64 = 4;
const PEER_BROADCAST: i64 = 8;
const PEER_GIGAGROUP: i64 = 12;

struct Cache {
    home_dc: i32,
    dc_options: HashMap<i32, DcOption>,
}

/// SQLx-backed implementation of the grammers session contract.
///
/// The schema intentionally matches `grammers-session`'s built-in SQLite
/// storage, so existing session files remain compatible while SQLx owns the
/// only SQLite linkage in the application.
pub struct SqlxSession {
    database: SqlitePool,
    cache: Mutex<Cache>,
    write_lock: AsyncMutex<()>,
}

#[derive(Debug)]
pub enum SqlxSessionError {
    Poisoned,
    Io(std::io::Error),
    AddrParse(AddrParseError),
    Sql(sqlx::Error),
    InvalidAuthKeyLength(usize),
}

impl std::error::Error for SqlxSessionError {}

impl fmt::Display for SqlxSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Poisoned => write!(f, "session lock is poisoned"),
            Self::Io(error) => write!(f, "session filesystem error: {error}"),
            Self::AddrParse(error) => write!(f, "invalid socket address syntax: {error}"),
            Self::Sql(error) => write!(f, "{error}"),
            Self::InvalidAuthKeyLength(actual) => {
                write!(f, "invalid auth_key length: expected 256, got {actual}")
            }
        }
    }
}

impl From<std::io::Error> for SqlxSessionError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<AddrParseError> for SqlxSessionError {
    fn from(error: AddrParseError) -> Self {
        Self::AddrParse(error)
    }
}

impl From<sqlx::Error> for SqlxSessionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

impl SqlxSession {
    /// Opens or creates a session database at `path`.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, SqlxSessionError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let database = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        initialize_schema(&database).await?;

        let defaults = SessionData::default();
        let home_dc = sqlx::query_scalar::<_, i32>("SELECT dc_id FROM dc_home LIMIT 1")
            .fetch_optional(&database)
            .await?
            .unwrap_or(defaults.home_dc);

        let mut dc_options = defaults.dc_options;
        for row in sqlx::query("SELECT dc_id, ipv4, ipv6, auth_key FROM dc_option")
            .fetch_all(&database)
            .await?
        {
            let auth_key = row
                .try_get::<Option<Vec<u8>>, _>("auth_key")?
                .map(|value| {
                    value.try_into().map_err(|value: Vec<u8>| {
                        SqlxSessionError::InvalidAuthKeyLength(value.len())
                    })
                })
                .transpose()?;
            let dc_option = DcOption {
                id: row.try_get("dc_id")?,
                ipv4: row.try_get::<String, _>("ipv4")?.parse::<SocketAddrV4>()?,
                ipv6: row.try_get::<String, _>("ipv6")?.parse::<SocketAddrV6>()?,
                auth_key,
            };
            dc_options.insert(dc_option.id, dc_option);
        }

        Ok(Self {
            database,
            cache: Mutex::new(Cache {
                home_dc,
                dc_options,
            }),
            write_lock: AsyncMutex::new(()),
        })
    }

    async fn load_peer(&self, peer: PeerId) -> Result<Option<PeerInfo>, SqlxSessionError> {
        let row = if let Some(peer_id) = peer.bot_api_dialog_id() {
            sqlx::query(
                "SELECT peer_id, hash, subtype
                 FROM peer_info WHERE peer_id = ?1 LIMIT 1",
            )
            .bind(peer_id)
            .fetch_optional(&self.database)
            .await?
        } else {
            sqlx::query(
                "SELECT peer_id, hash, subtype
                 FROM peer_info WHERE subtype & ?1 LIMIT 1",
            )
            .bind(PEER_USER_SELF)
            .fetch_optional(&self.database)
            .await?
        };

        let Some(row) = row else {
            return Ok(None);
        };
        let subtype = row.try_get::<Option<i64>, _>("subtype")?;
        let auth = row
            .try_get::<Option<i64>, _>("hash")?
            .map(PeerAuth::from_hash);

        let peer = match peer.kind() {
            PeerKind::User => PeerInfo::User {
                id: PeerId::user_unchecked(row.try_get("peer_id")?).bare_id_unchecked(),
                auth,
                bot: subtype.map(|value| value & PEER_USER_BOT != 0),
                is_self: subtype.map(|value| value & PEER_USER_SELF != 0),
            },
            PeerKind::Chat => PeerInfo::Chat {
                id: peer.bare_id_unchecked(),
            },
            PeerKind::Channel => PeerInfo::Channel {
                id: peer.bare_id_unchecked(),
                auth,
                kind: subtype.and_then(|value| {
                    if value & PEER_GIGAGROUP == PEER_GIGAGROUP {
                        Some(ChannelKind::Gigagroup)
                    } else if value & PEER_BROADCAST != 0 {
                        Some(ChannelKind::Broadcast)
                    } else if value & PEER_MEGAGROUP != 0 {
                        Some(ChannelKind::Megagroup)
                    } else {
                        None
                    }
                }),
            },
        };
        Ok(Some(peer))
    }
}

async fn initialize_schema(database: &SqlitePool) -> Result<(), SqlxSessionError> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS dc_home (
            dc_id INTEGER NOT NULL PRIMARY KEY
        )",
        "CREATE TABLE IF NOT EXISTS dc_option (
            dc_id INTEGER NOT NULL PRIMARY KEY,
            ipv4 TEXT NOT NULL,
            ipv6 TEXT NOT NULL,
            auth_key BLOB
        )",
        "CREATE TABLE IF NOT EXISTS peer_info (
            peer_id INTEGER NOT NULL PRIMARY KEY,
            hash INTEGER,
            subtype INTEGER
        )",
        "CREATE TABLE IF NOT EXISTS update_state (
            pts INTEGER NOT NULL,
            qts INTEGER NOT NULL,
            date INTEGER NOT NULL,
            seq INTEGER NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS channel_state (
            peer_id INTEGER NOT NULL PRIMARY KEY,
            pts INTEGER NOT NULL
        )",
    ] {
        sqlx::query(statement).execute(database).await?;
    }
    let user_version = sqlx::query_scalar::<_, i64>("PRAGMA user_version")
        .fetch_one(database)
        .await?;
    if user_version == 0 {
        // Match the version marker used by grammers-session's built-in storage.
        sqlx::query("PRAGMA user_version = 1")
            .execute(database)
            .await?;
    }
    Ok(())
}

impl Session for SqlxSession {
    type Error = SqlxSessionError;

    fn home_dc_id(&self) -> Result<i32, SqlxSessionError> {
        self.cache
            .lock()
            .map(|cache| cache.home_dc)
            .map_err(|_| SqlxSessionError::Poisoned)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), SqlxSessionError>> {
        Box::pin(async move {
            let _write_guard = self.write_lock.lock().await;
            let mut transaction = self.database.begin().await?;
            sqlx::query("DELETE FROM dc_home")
                .execute(&mut *transaction)
                .await?;
            sqlx::query("INSERT INTO dc_home (dc_id) VALUES (?1)")
                .bind(dc_id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;

            self.cache
                .lock()
                .map_err(|_| SqlxSessionError::Poisoned)?
                .home_dc = dc_id;
            Ok(())
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, SqlxSessionError> {
        self.cache
            .lock()
            .map_err(|_| SqlxSessionError::Poisoned)
            .map(|cache| cache.dc_options.get(&dc_id).cloned())
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), SqlxSessionError>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            let _write_guard = self.write_lock.lock().await;
            sqlx::query(
                "INSERT OR REPLACE INTO dc_option (dc_id, ipv4, ipv6, auth_key)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(dc_option.id)
            .bind(dc_option.ipv4.to_string())
            .bind(dc_option.ipv6.to_string())
            .bind(dc_option.auth_key.map(|key| key.to_vec()))
            .execute(&self.database)
            .await?;

            self.cache
                .lock()
                .map_err(|_| SqlxSessionError::Poisoned)?
                .dc_options
                .insert(dc_option.id, dc_option);
            Ok(())
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, SqlxSessionError>> {
        Box::pin(self.load_peer(peer))
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), SqlxSessionError>> {
        let peer = peer.clone();
        Box::pin(async move {
            let _write_guard = self.write_lock.lock().await;
            let peer = if let Some(mut existing) = self.load_peer(peer.id()).await? {
                existing.extend_info(&peer);
                existing
            } else {
                peer
            };
            let subtype = match &peer {
                PeerInfo::User { bot, is_self, .. } => {
                    match (bot.unwrap_or_default(), is_self.unwrap_or_default()) {
                        (true, true) => Some(PEER_USER_SELF | PEER_USER_BOT),
                        (true, false) => Some(PEER_USER_BOT),
                        (false, true) => Some(PEER_USER_SELF),
                        (false, false) => None,
                    }
                }
                PeerInfo::Chat { .. } => None,
                PeerInfo::Channel { kind, .. } => kind.map(|kind| match kind {
                    ChannelKind::Megagroup => PEER_MEGAGROUP,
                    ChannelKind::Broadcast => PEER_BROADCAST,
                    ChannelKind::Gigagroup => PEER_GIGAGROUP,
                }),
            };
            sqlx::query(
                "INSERT OR REPLACE INTO peer_info (peer_id, hash, subtype)
                 VALUES (?1, ?2, ?3)",
            )
            .bind(peer.id().bot_api_dialog_id_unchecked())
            .bind(peer.auth().map(|auth| auth.hash()))
            .bind(subtype)
            .execute(&self.database)
            .await?;
            Ok(())
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, SqlxSessionError>> {
        Box::pin(async move {
            let mut state = sqlx::query("SELECT pts, qts, date, seq FROM update_state LIMIT 1")
                .fetch_optional(&self.database)
                .await?
                .map(|row| {
                    Ok::<_, SqlxSessionError>(UpdatesState {
                        pts: row.try_get("pts")?,
                        qts: row.try_get("qts")?,
                        date: row.try_get("date")?,
                        seq: row.try_get("seq")?,
                        channels: Vec::new(),
                    })
                })
                .transpose()?
                .unwrap_or_default();
            state.channels = sqlx::query("SELECT peer_id, pts FROM channel_state")
                .fetch_all(&self.database)
                .await?
                .into_iter()
                .map(|row| {
                    Ok::<_, SqlxSessionError>(ChannelState {
                        id: row.try_get("peer_id")?,
                        pts: row.try_get("pts")?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(state)
        })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<(), SqlxSessionError>> {
        Box::pin(async move {
            let _write_guard = self.write_lock.lock().await;
            let mut transaction = self.database.begin().await?;
            match update {
                UpdateState::All(state) => {
                    sqlx::query("DELETE FROM update_state")
                        .execute(&mut *transaction)
                        .await?;
                    sqlx::query(
                        "INSERT INTO update_state (pts, qts, date, seq)
                         VALUES (?1, ?2, ?3, ?4)",
                    )
                    .bind(state.pts)
                    .bind(state.qts)
                    .bind(state.date)
                    .bind(state.seq)
                    .execute(&mut *transaction)
                    .await?;
                    sqlx::query("DELETE FROM channel_state")
                        .execute(&mut *transaction)
                        .await?;
                    for channel in state.channels {
                        sqlx::query("INSERT INTO channel_state (peer_id, pts) VALUES (?1, ?2)")
                            .bind(channel.id)
                            .bind(channel.pts)
                            .execute(&mut *transaction)
                            .await?;
                    }
                }
                UpdateState::Primary { pts, date, seq } => {
                    let result =
                        sqlx::query("UPDATE update_state SET pts = ?1, date = ?2, seq = ?3")
                            .bind(pts)
                            .bind(date)
                            .bind(seq)
                            .execute(&mut *transaction)
                            .await?;
                    if result.rows_affected() == 0 {
                        sqlx::query(
                            "INSERT INTO update_state (pts, qts, date, seq)
                             VALUES (?1, 0, ?2, ?3)",
                        )
                        .bind(pts)
                        .bind(date)
                        .bind(seq)
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
                UpdateState::Secondary { qts } => {
                    let result = sqlx::query("UPDATE update_state SET qts = ?1")
                        .bind(qts)
                        .execute(&mut *transaction)
                        .await?;
                    if result.rows_affected() == 0 {
                        sqlx::query(
                            "INSERT INTO update_state (pts, qts, date, seq)
                             VALUES (0, ?1, 0, 0)",
                        )
                        .bind(qts)
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
                UpdateState::Channel { id, pts } => {
                    sqlx::query(
                        "INSERT OR REPLACE INTO channel_state (peer_id, pts)
                         VALUES (?1, ?2)",
                    )
                    .bind(id)
                    .bind(pts)
                    .execute(&mut *transaction)
                    .await?;
                }
            }
            transaction.commit().await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grammers_session::Session;
    use tempfile::tempdir;

    #[tokio::test]
    async fn persists_grammers_session_state() {
        let directory = tempdir().expect("create temporary session directory");
        let path = directory.path().join("grammers.session.sqlite3");
        let session = SqlxSession::open(&path).await.expect("open session");

        let default_home_dc = session.home_dc_id().expect("read home DC");
        session
            .set_home_dc_id(default_home_dc + 1)
            .await
            .expect("write home DC");
        assert_eq!(
            session.home_dc_id().expect("read updated home DC"),
            default_home_dc + 1
        );

        let dc_option = DcOption {
            id: 99,
            ipv4: "127.0.0.1:443".parse().expect("IPv4 address"),
            ipv6: "[::1]:443".parse().expect("IPv6 address"),
            auth_key: Some([7; 256]),
        };
        session
            .set_dc_option(&dc_option)
            .await
            .expect("write DC option");
        assert_eq!(
            session.dc_option(99).expect("read DC option"),
            Some(dc_option.clone())
        );

        session
            .cache_peer(&PeerInfo::User {
                id: 123,
                auth: Some(PeerAuth::from_hash(456)),
                bot: Some(true),
                is_self: Some(true),
            })
            .await
            .expect("cache self user");
        assert_eq!(
            session
                .peer(PeerId::self_user())
                .await
                .expect("read self user"),
            Some(PeerInfo::User {
                id: 123,
                auth: Some(PeerAuth::from_hash(456)),
                bot: Some(true),
                is_self: Some(true),
            })
        );

        let state = UpdatesState {
            pts: 1,
            qts: 2,
            date: 3,
            seq: 4,
            channels: vec![ChannelState { id: 55, pts: 6 }],
        };
        session
            .set_update_state(UpdateState::All(state.clone()))
            .await
            .expect("write update state");
        assert_eq!(
            session.updates_state().await.expect("read update state"),
            state
        );
        drop(session);

        let reopened = SqlxSession::open(&path).await.expect("reopen session");
        assert_eq!(
            reopened.home_dc_id().expect("read persisted home DC"),
            default_home_dc + 1
        );
        assert_eq!(
            reopened.dc_option(99).expect("read persisted DC option"),
            Some(dc_option)
        );
        assert_eq!(
            reopened
                .peer(PeerId::self_user())
                .await
                .expect("read persisted self user"),
            Some(PeerInfo::User {
                id: 123,
                auth: Some(PeerAuth::from_hash(456)),
                bot: Some(true),
                is_self: Some(true),
            })
        );
        assert_eq!(
            reopened
                .updates_state()
                .await
                .expect("read persisted update state"),
            state
        );
    }
}
