#! /bin/bash

cargo fmt
cd bridge && npx prettier --write 'contracts/**/*.sol'


