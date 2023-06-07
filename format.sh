#! /bin/bash

cargo fmt
npx buf format -w src/proto/proto
cd bridge && npx prettier --write 'contracts/**/*.sol'


