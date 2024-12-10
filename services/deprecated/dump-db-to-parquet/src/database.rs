// database.rs

use chrono::NaiveDateTime;
use rusqlite::{params, Connection, Result};

#[derive(Debug)]
pub struct FirehosePost {
    uri: String,
    created_at: String,
    text: String,
    embed: String,
    langs: Option<String>,
    entities: Option<String>,
    facets: Option<String>,
    labels: Option<String>,
    reply: Option<String>,
    reply_parent: Option<String>,
    reply_root: Option<String>,
    tags: Option<String>,
    py_type: String,
    cid: String,
    author: String,
    indexed_at: NaiveDateTime,
    synctimestamp: String,
}

pub fn fetch_posts(db_path: &str, start: NaiveDateTime, end: NaiveDateTime) -> Result<Vec<FirehosePost>> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare(
        "SELECT uri, created_at, text, embed, langs, entities, facets, labels, reply, reply_parent, reply_root, tags, py_type, cid, author, indexed_at, synctimestamp FROM firehoseposts WHERE indexed_at >= ? AND indexed_at < ?",
    )?;
    let posts_iter = stmt.query_map(params![start, end], |row| {
        Ok(FirehosePost {
            uri: row.get(0)?,
            created_at: row.get(1)?,
            text: row.get(2)?,
            embed: row.get(3)?,
            langs: row.get(4)?,
            entities: row.get(5)?,
            facets: row.get(6)?,
            labels: row.get(7)?,
            reply: row.get(8)?,
            reply_parent: row.get(9)?,
            reply_root: row.get(10)?,
            tags: row.get(11)?,
            py_type: row.get(12)?,
            cid: row.get(13)?,
            author: row.get(14)?,
            indexed_at: row.get(15)?,
            synctimestamp: row.get(16)?,
        })
    })?;

    posts_iter.collect()
}
