use crate::{bucket::Bucket, evt::Event};

/// Gets sub buckets from a db and gets some of them.
///
/// # Arguments
/// - shared_db: The db which may contain sub buckets.
/// - get_sub_buckets: Gets sub buckets.
/// - filter: Gets filtered sub buckets.
/// - filter_config: The config to filter buckets.
/// - push_down: Use remote filtering.
/// - double_check: Use local filtering.
///
/// # Local filtering
///
/// | push_down | double_check | Local filtering | Overview                      |
/// |:---------:|:------------:|:---------------:|:-----------------------------:|
/// | false     | false        | true            | Gets All -> Local filter      |
/// | false     | true         | true            | Gets All -> Local filter      |
/// | true      | false        | false           | Gets filtered                 |
/// | true      | true         | true            | Gets filtered -> Local filter |
///
pub fn get_sub_buckets<D, C, S, F, P>(
    shared_db: &mut D,
    b: &Bucket,
    get_sub_buckets: &mut P,
    filter: &F,
    filter_config: &C,
    push_down: bool,
    double_check: bool,
) -> Result<Vec<S>, Event>
where
    P: FnMut(&mut D, &Bucket, Option<&C>) -> Result<Vec<S>, Event>,
    F: Fn(Vec<S>, &C) -> Vec<S>,
{
    let get_all: bool = !push_down;
    let sub_buckets: Vec<S> = match get_all {
        true => get_sub_buckets(shared_db, b, None)?,
        false => get_sub_buckets(shared_db, b, Some(filter_config))?,
    };
    let local_filter_required: bool = get_all || double_check;
    match local_filter_required {
        true => Ok(filter(sub_buckets, filter_config)),
        false => Ok(sub_buckets),
    }
}

/// Creates a new closure which gets sub buckets.
///
/// # Arguments
/// - get_sub: Gets sub buckets.
/// - filter: Gets filtered sub buckets.
/// - pushdown: Checks if a remote filter must be used or not.
pub fn get_sub_buckets_new<D, C, S, F, P, G>(
    mut get_sub: G,
    filter: F,
    pushdown: P,
) -> impl FnMut(&mut D, &Bucket, &C, bool) -> Result<Vec<S>, Event>
where
    P: Fn(&C) -> bool,
    G: FnMut(&mut D, &Bucket, Option<&C>) -> Result<Vec<S>, Event>,
    F: Fn(Vec<S>, &C) -> Vec<S>,
{
    move |shared: &mut D, b: &Bucket, cfg: &C, double_check: bool| {
        let remote_check: bool = pushdown(cfg);
        get_sub_buckets(
            shared,
            b,
            &mut get_sub,
            &filter,
            cfg,
            remote_check,
            double_check,
        )
    }
}

/// Creates a closure which checks if a remote filter must be used or not.
///
/// # Arguments
/// - estimate_ix_scan: Gets number of index scans.
/// - estimate_sq_scan: Gets number of sequential scans.
/// - ix_scan_cost: The cost to get a row(random scan).
/// - sq_scan_cost: The cost to get a row(sequential scan).
pub fn pushdown_by_storage_new<C, I, R>(
    estimate_ix_scan: I,
    estimate_sq_scan: R,
    ix_scan_cost: f32,
    sq_scan_cost: f32,
) -> impl Fn(&C) -> bool
where
    I: Fn(&C) -> f32,
    R: Fn(&C) -> f32,
{
    move |filter_cfg: &C| {
        let ix_cost: f32 = ix_scan_cost * estimate_ix_scan(filter_cfg);
        let sq_cost: f32 = sq_scan_cost * estimate_sq_scan(filter_cfg);
        let scan_all: bool = sq_cost < ix_cost;
        let filter_by_remote: bool = !scan_all;
        filter_by_remote
    }
}

#[cfg(test)]
mod test_sub {

    mod get_sub_buckets {

        use crate::bucket::Bucket;
        use crate::sub::get_sub_buckets;

        #[derive(PartialEq, Eq, Debug)]
        struct SubBucket {
            id: u16,
            data: Vec<u8>,
        }

        #[derive(Default)]
        struct Filter {
            id_lbi: u16,
            id_ubi: u16,
        }

        #[test]
        fn test_empty() {
            let mut dummy: u8 = 0;
            let b: Bucket =
                Bucket::new_checked("items_2023_01_01_cafef00ddeadbeafface864299792458".into());

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| Ok(vec![]),
                &mut |v: Vec<SubBucket>, _: &Filter| v,
                &Filter::default(),
                false,
                false,
            )
            .unwrap();
            assert_eq!(v, vec![]);

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| Ok(vec![]),
                &mut |v: Vec<SubBucket>, _: &Filter| v,
                &Filter::default(),
                false,
                true,
            )
            .unwrap();
            assert_eq!(v, vec![]);

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| Ok(vec![]),
                &mut |v: Vec<SubBucket>, _: &Filter| v,
                &Filter::default(),
                true,
                false,
            )
            .unwrap();
            assert_eq!(v, vec![]);

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| Ok(vec![]),
                &mut |v: Vec<SubBucket>, _: &Filter| v,
                &Filter::default(),
                true,
                true,
            )
            .unwrap();
            assert_eq!(v, vec![]);
        }

        #[test]
        fn test_local_check() {
            let mut dummy: u8 = 0;
            let b: Bucket =
                Bucket::new_checked("items_2023_01_01_cafef00ddeadbeafface864299792458".into());

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| {
                    Ok(vec![
                        SubBucket {
                            id: 0x0042,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0043,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0044,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0045,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0046,
                            data: vec![],
                        },
                    ])
                },
                &mut |v: Vec<SubBucket>, f: &Filter| {
                    v.into_iter()
                        .filter(|s| {
                            let lbi: u16 = f.id_lbi;
                            let ubi: u16 = f.id_ubi;
                            let id: u16 = s.id;
                            lbi <= id && id <= ubi
                        })
                        .collect()
                },
                &Filter {
                    id_lbi: 0x0043,
                    id_ubi: 0x0045,
                },
                false,
                false,
            )
            .unwrap();

            assert_eq!(v.len(), 3);
        }

        #[test]
        fn test_pushdown_only() {
            let mut dummy: u8 = 0;
            let b: Bucket =
                Bucket::new_checked("items_2023_01_01_cafef00ddeadbeafface864299792458".into());

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| {
                    Ok(vec![
                        SubBucket {
                            id: 0x0042,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0043,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0044,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0045,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0046,
                            data: vec![],
                        },
                    ])
                },
                &mut |v: Vec<SubBucket>, f: &Filter| {
                    v.into_iter()
                        .filter(|s| {
                            let lbi: u16 = f.id_lbi;
                            let ubi: u16 = f.id_ubi;
                            let id: u16 = s.id;
                            lbi <= id && id <= ubi
                        })
                        .collect()
                },
                &Filter {
                    id_lbi: 0x0043,
                    id_ubi: 0x0045,
                },
                true,
                false,
            )
            .unwrap();

            assert_eq!(v.len(), 5);
        }

        #[test]
        fn test_double_check() {
            let mut dummy: u8 = 0;
            let b: Bucket =
                Bucket::new_checked("items_2023_01_01_cafef00ddeadbeafface864299792458".into());

            let v: Vec<SubBucket> = get_sub_buckets(
                &mut dummy,
                &b,
                &mut |_: &mut u8, _: &Bucket, _: Option<&Filter>| {
                    Ok(vec![
                        SubBucket {
                            id: 0x0042,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0043,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0044,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0045,
                            data: vec![],
                        },
                        SubBucket {
                            id: 0x0046,
                            data: vec![],
                        },
                    ])
                },
                &mut |v: Vec<SubBucket>, f: &Filter| {
                    v.into_iter()
                        .filter(|s| {
                            let lbi: u16 = f.id_lbi;
                            let ubi: u16 = f.id_ubi;
                            let id: u16 = s.id;
                            lbi <= id && id <= ubi
                        })
                        .collect()
                },
                &Filter {
                    id_lbi: 0x0043,
                    id_ubi: 0x0045,
                },
                true,
                true,
            )
            .unwrap();

            assert_eq!(v.len(), 3);
        }
    }
}
