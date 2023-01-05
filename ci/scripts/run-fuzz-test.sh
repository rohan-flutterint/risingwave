#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

if [[ "$RUN_SQLSMITH" -eq "1" ]]; then
    while getopts 'p:' opt; do
        case ${opt} in
            p )
                profile=$OPTARG
                ;;
            \? )
                echo "Invalid Option: -$OPTARG" 1>&2
                exit 1
                ;;
            : )
                echo "Invalid option: $OPTARG requires an argument" 1>&2
                ;;
        esac
    done
    shift $((OPTIND -1))

    echo "--- Download artifacts"
    mkdir -p target/debug
    buildkite-agent artifact download risingwave-"$profile" target/debug/
    buildkite-agent artifact download risedev-dev-"$profile" target/debug/
    mv target/debug/risingwave-"$profile" target/debug/risingwave
    mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev

    echo "--- Adjust permission"
    chmod +x ./target/debug/risingwave
    chmod +x ./target/debug/risedev-dev

    echo "--- Generate RiseDev CI config"
    cp ci/risedev-components.ci.env risedev-components.user.env

    echo "--- Prepare RiseDev dev cluster"
    cargo make pre-start-dev
    cargo make link-all-in-one-binaries

    echo "--- frontend, fuzzing"
    NEXTEST_PROFILE=ci cargo nextest run run_sqlsmith_on_frontend --features "failpoints sync_point enable_sqlsmith_unit_test" 2> >(tee);

    echo "--- e2e, ci-3cn-1fe, fuzzing"
    buildkite-agent artifact download sqlsmith-"$profile" target/debug/
    mv target/debug/sqlsmith-"$profile" target/debug/sqlsmith
    chmod +x ./target/debug/sqlsmith

    cargo make ci-start ci-3cn-1fe
    RUST_LOG=info timeout 20m ./target/debug/sqlsmith test --count "$SQLSMITH_COUNT" --testdata ./src/tests/sqlsmith/tests/testdata

    # Using `kill` instead of `ci-kill` avoids storing excess logs.
    # If there's errors, the failing query will be printed to stderr.
    # Use that to reproduce logs on local machine.
    echo "--- Kill cluster"
    cargo make kill
fi
