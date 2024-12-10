mod database;
mod parquet_writer;

use chrono::Utc;
use database::fetch_posts;
use parquet_writer::write_to_parquet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yesterday = Utc::now() - chrono::Duration::days(1);
    let today = Utc::now();
    let filename = format!("{}.parquet", yesterday.format("%Y-%m-%d"));

    let posts = fetch_posts("firehose.db", yesterday.naive_utc(), today.naive_utc())?;
    write_to_parquet(&filename, &posts)?;

    Ok(())
}
