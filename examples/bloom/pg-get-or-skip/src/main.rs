use std::collections::BTreeMap;
use std::env;

use rs_bucket_filter::{
    bloom::{bloom_check_new, get_or_skip_if_missing, update_bloom_bits, BloomResult},
    bucket::Bucket,
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

struct FilterDatabase {
    datname: String,
}

fn db_hash(f: &FilterDatabase) -> BloomBits {
    match f.datname.as_str() {
        "postgres" => BloomBits {
            packed: [0x333, 0x634],
        },
        "template0" => BloomBits {
            packed: [0x599, 0x3776], // hash collision
        },
        "templateZ" => BloomBits {
            packed: [0x599, 0x3776], // hash collision
        },
        _ => BloomBits::default(),
    }
}

fn db_check(a: &BloomBits, b: &BloomBits) -> BloomResult {
    let and: BloomBits = a.and(b);
    match and.eq(b) {
        true => BloomResult::MayExists,
        false => BloomResult::Missing,
    }
}

fn db_bloom() -> impl Fn(&BTreeMap<Bucket, BloomBits>, &FilterDatabase, &Bucket) -> BloomResult {
    bloom_check_new(db_hash, db_check)
}

fn pg_get_db(c: &mut Client, b: &Bucket, f: &FilterDatabase) -> Result<Vec<String>, Event> {
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
        .map_err(|e| Event::UnexpectedError(format!("Unable to get strings: {}", e)))?;
    (raws.is_empty()).then(|| {
        eprintln!(
            "false positive. bucket={}, datname={}",
            b.as_str(),
            f.datname
        )
    });
    Ok(raws.into_iter().flat_map(|r: Row| r.try_get(0)).collect())
}

fn get_or_skip_if_missing_new<B>(
) -> impl Fn(&B, &mut Client, &Bucket, &FilterDatabase) -> Result<Vec<String>, Event>
where
    B: Fn(&Bucket, &FilterDatabase) -> BloomResult,
{
    move |bloom: &B, c: &mut Client, b: &Bucket, f: &FilterDatabase| {
        get_or_skip_if_missing(bloom, c, b, &mut pg_get_db, f)
    }
}

#[derive(PartialEq, Eq, Default)]
struct BloomBits {
    packed: [u128; 2], // 256 bits
}

impl BloomBits {
    fn and_lo(&self, other: &Self) -> u128 {
        let sl = self.packed[0];
        let ol = other.packed[0];
        sl & ol
    }

    fn and_hi(&self, other: &Self) -> u128 {
        let sh = self.packed[1];
        let oh = other.packed[1];
        sh & oh
    }

    fn and(&self, other: &Self) -> Self {
        let lo: u128 = self.and_lo(other);
        let hi: u128 = self.and_hi(other);
        Self { packed: [lo, hi] }
    }
}

fn sub() -> Result<(), Event> {
    let mut c: Client = pg_client_new()?;

    let mut bloom_bits: BTreeMap<Bucket, BloomBits> = BTreeMap::new();
    let mut dummy_bloom_getter = |_: &mut Client, _b: &Bucket| {
        Ok(vec![
            (
                Bucket::new_checked("pg_database".into()),
                BloomBits {
                    packed: [0x333, 0x634],
                },
            ),
            (
                Bucket::new_checked("pg_database".into()),
                BloomBits {
                    packed: [0x333 | 0x599, 0x634 | 0x3776],
                },
            ),
        ])
    };
    update_bloom_bits(
        &mut bloom_bits,
        &mut c,
        &mut dummy_bloom_getter,
        &Bucket::new_checked("bloom_2022_12_27".into()),
    )?;
    // |&BTreeMap<Bucket, BloomBits>, &FilterDatabase, &Bucket| BloomResult
    println!("bloom count: {}", bloom_bits.len());
    let bloom = db_bloom();
    let blm = |b: &Bucket, f: &FilterDatabase| bloom(&bloom_bits, f, b);
    let getter = get_or_skip_if_missing_new();

    let strings: Vec<String> = getter(
        &blm,
        &mut c,
        &Bucket::new_checked("pg_database".into()),
        &FilterDatabase {
            datname: "postgres".into(),
        },
    )?;
    println!("str len: {}", strings.len());
    for s in strings {
        println!("s: {}", s);
    }

    let strings: Vec<String> = getter(
        &blm,
        &mut c,
        &Bucket::new_checked("pg_database".into()),
        &FilterDatabase {
            datname: "templateZ".into(),
        },
    )?;
    println!("str len: {}", strings.len());
    for s in strings {
        println!("s: {}", s);
    }
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
