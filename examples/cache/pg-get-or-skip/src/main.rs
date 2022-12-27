use std::collections::BTreeSet;
use std::env;

use rs_bucket_filter::{
    bucket::Bucket,
    cache::{get_or_skip_if_bucket_missing, update_cache_btree},
    evt::Event,
};

use postgres::{Client, Config, NoTls, Row};

fn pg_client_new() -> Result<Client, Event> {
    Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap().as_str())
        .application_name(env::var("PGAPPNAME").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .port(5432)
        .connect(NoTls)
        .map_err(|e| Event::UnableToConnect(format!("Unable to connect to postgres: {}", e)))
}

fn pg_get_buckets(c: &mut Client) -> Result<Vec<String>, Event> {
    let query: &str = r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'pg_catalog'
        ORDER BY table_name
    "#;
    let raws: Vec<Row> = c
        .query(query, &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to get table names: {}", e)))?;
    let noerr: Vec<String> = raws
        .into_iter()
        .flat_map(|r: Row| r.try_get(0).ok())
        .collect();
    Ok(noerr)
}

struct FilterDatabase {
    datname: String,
}

fn pg_get_database(c: &mut Client, b: &Bucket, f: &FilterDatabase) -> Result<Vec<String>, Event> {
    let query: String = format!(
        r#"
            SELECT datcollate
            FROM {}
            WHERE datname = $1::TEXT
            LIMIT 1
        "#,
        b.as_str(),
    );
    let raws: Vec<Row> = c
        .query(query.as_str(), &[&f.datname])
        .map_err(|e| Event::UnexpectedError(format!("Unable to get a string: {}", e)))?;
    Ok(raws.into_iter().flat_map(|r: Row| r.try_get(0)).collect())
}

fn sub() -> Result<(), Event> {
    let mut c: Client = pg_client_new()?;
    let mut cache: BTreeSet<Bucket> = BTreeSet::new();
    update_cache_btree(&mut cache, &mut c, &mut pg_get_buckets)?;
    for b in cache.iter().take(3) {
        let name: &str = b.as_str();
        println!("bucket name: {}", name);
    }

    let f_cache = |n: &Bucket| cache.contains(n);
    let bucket: Bucket = Bucket::new_checked("pg_database".into());
    let filter = FilterDatabase {
        datname: "template0".into(),
    };
    let strings: Vec<String> =
        get_or_skip_if_bucket_missing(&f_cache, &mut c, &bucket, &mut pg_get_database, &filter)?;
    for s in strings {
        println!("s: {}", s);
    }
    let bucket_dummy: Bucket = Bucket::new_checked("pg_database_NOT_EXIST".into());
    let empty_strings: Vec<String> = get_or_skip_if_bucket_missing(
        &f_cache,
        &mut c,
        &bucket_dummy,
        &mut pg_get_database,
        &filter,
    )?;
    println!("must be empty: {}", empty_strings.len());
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
