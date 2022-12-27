use std::collections::BTreeSet;

use crate::{bucket::Bucket, evt::Event};

/// Scans from a slow db if a bucket is in a cache.
///
/// # Arguments
///
/// - cache: Checks if a bucket exists.
/// - shared_db: The slow db to get values.
/// - bucket: The bucket which may contain values.
/// - getter: Gets values from a bucket.
/// - filter: The filter to get values from a bucket.
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
/// #### cache
///
/// ```toml
/// [[cache.buckets_2022_12_27]]
/// key = "dafef00ddeadbeafface864299792458"
/// ```
///
/// ```toml
/// [[cache.buckets_2022_12_27]]
/// key = "cafef00ddeadbeafface864299792458"
/// ```
///
/// #### filter
///
/// ```toml
/// [filter]
/// bucket = "cafef00ddeadbeafface864299792458"
/// item_id = "4589506252015"
/// scan count = 1
/// result = ...
/// ```
///
/// ```toml
/// [filter]
/// bucket = "eafef00ddeadbeafface864299792458"
/// item_id = "4589506252015"
/// scan count = 0(skipped)
/// result = (no hits)
/// ```
pub fn get_or_skip_if_bucket_missing<C, D, G, F, T>(
    cache: &C,
    shared_db: &mut D,
    bucket: &Bucket,
    getter: &mut G,
    filter: &F,
) -> Result<Vec<T>, Event>
where
    C: Fn(&Bucket) -> bool,
    G: FnMut(&mut D, &Bucket, &F) -> Result<Vec<T>, Event>,
{
    let bucket_exists: bool = cache(bucket);
    match bucket_exists {
        true => getter(shared_db, bucket, filter),
        false => Ok(vec![]),
    }
}

/// Gets list of buckets and updates the cache of buckets.
///
/// # Arguments
/// - cache: The cache to be updated.
/// - shared_db: The db which contains buckets.
/// - list_buckets: Gets the list of buckets from the shared db.
pub fn update_cache_btree<D, L>(
    cache: &mut BTreeSet<Bucket>,
    shared_db: &mut D,
    list_buckets: &mut L,
) -> Result<u64, Event>
where
    L: FnMut(&mut D) -> Result<Vec<String>, Event>,
{
    cache.clear();
    let bucket_names: Vec<String> = list_buckets(shared_db)?;
    let buckets = bucket_names.into_iter().map(Bucket::new_checked);
    Ok(buckets.fold(0, |tot, bucket| {
        let inserted: bool = cache.insert(bucket);
        inserted.then_some(1).map(|cnt| cnt + tot).unwrap_or(tot)
    }))
}
