#!/bin/sh

export CARGO_INCREMENTAL=0
export RUSTC_BOOTSTRAP=1

export RUSTFLAGS=""
export RUSTFLAGS="$RUSTFLAGS -Zprofile"
export RUSTFLAGS="$RUSTFLAGS -Ccodegen-units=1"
export RUSTFLAGS="$RUSTFLAGS -Cinline-threshold=0"
export RUSTFLAGS="$RUSTFLAGS -Copt-level=0"
export RUSTFLAGS="$RUSTFLAGS -Clink-dead-code"
export RUSTFLAGS="$RUSTFLAGS -Coverflow-checks=off"
export RUSTFLAGS="$RUSTFLAGS -Zpanic_abort_tests"
export RUSTFLAGS="$RUSTFLAGS -Cpanic=abort"
export RUSTFLAGS="$RUSTFLAGS -Cinstrument-coverage=all"

export RUSTDOCFLAGS=""
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Zprofile"
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Ccodegen-units=1"
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Cinline-threshold=0"
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Clink-dead-code"
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Coverflow-checks=off"
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Cpanic=abort"
export RUSTDOCFLAGS="$RUSTDOCFLAGS -Zpanic_abort_tests"

cargo build --verbose $CARGO_OPTIONS

rm -f *.profraw
export LLVM_PROFILE_FILE='prefix-%p-%m.profraw'

rm --force --recursive ./target/debug

cargo test --verbose $CARGO_OPTIONS -- --include-ignored

grcov . \
  --source-dir . \
  --binary-path ./target/debug/ \
  --output-type lcov \
  --branch \
  --ignore-not-existing \
  --output-path ./target/debug/lcov.info

genhtml \
  --output ./target/debug/coverage/ \
  --show-details \
  --highlight \
  --ignore-errors source \
  --legend ./target/debug/lcov.info \
  --css-file ./cov.css

