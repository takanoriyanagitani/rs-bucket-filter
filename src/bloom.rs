//! Filters buckets using bloom(like) filter.

use std::collections::BTreeMap;

use crate::{bucket::Bucket, evt::Event};

/// List of bloom check results.
pub enum BloomResult {
    /// An item may exist.
    MayExist,

    /// An item does not exist.
    Missing,
}

/// Gets values from a slow db if the values may exists.
///
/// # Arguments
/// - bloom: Checks if values may exists or not.
/// - shared_db: The db which may contain values.
/// - bucket: The bucket which may contain values.
/// - getter: Tries to get values from a bucket.
/// - filter: The filter to get values.
///
/// ## Sample
///
/// #### slow db
///
/// ```toml
/// [[slowdb.bucket_2022_12_27_cafef00ddeadbeafface864299792458]]
/// key = "07:58:11.0Z"
/// val.item_id = "4589506252015"
/// val.quantity = 3
/// val.weight = "500g"
/// val.tag = [
///   "water",
///   "drink",
///   "pet",
/// ]
/// ```
///
/// ```toml
/// [[slowdb.bucket_2022_12_27_dafef00ddeadbeafface864299792458]]
/// key = "07:58:11.0Z"
/// val.item_id = "4589506252015"
/// val.quantity = 3
/// val.weight = "500g"
/// val.tag = [
///   "water",
///   "drink",
///   "pet",
/// ]
/// ```
///
/// #### bloom
///
/// ```toml
/// [[fastdb.buckets_2022_12_27]]
/// key = "dafef00ddeadbeafface864299792458"
/// val = (... bloom bits; positive = 1,3,5,7,9)
/// ```
///
/// ```toml
/// [[fastdb.buckets_2022_12_27]]
/// key = "cafef00ddeadbeafface864299792458"
/// val = (... bloom bits; false positive = 2,3,5,7,9)
/// ```
///
/// #### filter
///
/// ```toml
/// [filter]
/// bucket = "cafef00ddeadbeafface864299792458"
/// item_id = "4589506252015"
/// bloom bits = 3
/// scan count = 1
/// result = ...
/// ```
///
/// ```toml
/// [filter]
/// bucket = "dafef00ddeadbeafface864299792458"
/// item_id = "4589506252015"
/// bloom bits = 42
/// scan count = 0(skipped)
/// result = (no hits)
/// ```
///
/// ```toml
/// [filter]
/// bucket = "cafef00ddeadbeafface864299792458"
/// item_id = "4589506252015"
/// bloom bits = 2
/// scan count = 1(false positive)
/// result = (no hits)
/// ```
pub fn get_or_skip_if_missing<B, D, G, F, T>(
    bloom: &B,
    shared_db: &mut D,
    bucket: &Bucket,
    getter: &mut G,
    filter: &F,
) -> Result<Vec<T>, Event>
where
    B: Fn(&Bucket, &F) -> BloomResult,
    G: FnMut(&mut D, &Bucket, &F) -> Result<Vec<T>, Event>,
{
    match bloom(bucket, filter) {
        BloomResult::Missing => Ok(vec![]),
        BloomResult::MayExist => getter(shared_db, bucket, filter),
    }
}

/// Gets bloom bits and updates the bloom bits container.
///
/// # Arguments
/// - bloom_bits: The bloom bits container to be updated.
/// - shared_db: The db which contains bloom bits.
/// - get_bloom_bits: Gets bloom bits for each bucket.
/// - bloom_bucket: The bucket which contains bloom bits for each bucket.
pub fn update_bloom_bits<D, B, G>(
    bloom_bits: &mut BTreeMap<Bucket, B>,
    shared_db: &mut D,
    get_bloom_bits: &mut G,
    bloom_bucket: &Bucket,
) -> Result<u64, Event>
where
    G: FnMut(&mut D, &Bucket) -> Result<Vec<(Bucket, B)>, Event>,
{
    bloom_bits.clear();
    let v: Vec<_> = get_bloom_bits(shared_db, bloom_bucket)?;
    Ok(v.into_iter().fold(0, |tot, pair| {
        let (bucket, bits) = pair;
        match bloom_bits.insert(bucket, bits) {
            None => 1 + tot,
            Some(_) => tot,
        }
    }))
}

/// Checks if values may exists or not.
///
/// # Arguments
/// - bloom_bits: Contains bloom bits for each bucket.
/// - hash: Computes the hash to be compared.
/// - filter: The filter to compute a hash.
/// - check: Checks if values may exists or not.
/// - b: The bucket which may contain values.
pub fn bloom_check<B, H, F, C>(
    bloom_bits: &BTreeMap<Bucket, B>,
    hash: &H,
    filter: &F,
    check: &C,
    b: &Bucket,
) -> BloomResult
where
    H: Fn(&F) -> B,
    C: Fn(&B, &B) -> BloomResult,
{
    let bloom_b: Option<&B> = bloom_bits.get(b);
    match bloom_b {
        None => BloomResult::Missing,
        Some(found) => {
            let computed: B = hash(filter);
            check(found, &computed)
        }
    }
}

/// Creates new checker which uses closures to compute hash / check bloom bits.
pub fn bloom_check_new<B, H, F, C>(
    hash: H,
    check: C,
) -> impl Fn(&BTreeMap<Bucket, B>, &F, &Bucket) -> BloomResult
where
    H: Fn(&F) -> B,
    C: Fn(&B, &B) -> BloomResult,
{
    move |bits: &BTreeMap<Bucket, B>, filter: &F, b: &Bucket| {
        bloom_check(bits, &hash, filter, &check, b)
    }
}
