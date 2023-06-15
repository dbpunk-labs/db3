#! /bin/bash

cargo fmt
npx buf format -w src/proto/proto
#cd metadata && npx prettier --write 'contracts/**/*.sol'


