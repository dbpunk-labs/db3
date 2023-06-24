#! /bin/bash
WORKSPACE=`pwd`
cargo fmt
npx buf format -w src/proto/proto
cd ${WORKSPACE}/metadata && npx prettier --write 'contracts/**/*.sol'
cd ${WORKSPACE}/sdk && yarn prettier --write src tests ./jest.setup.ts ./jest.config.ts
