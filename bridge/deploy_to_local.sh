#! /bin/sh
#
# deploy_to_scoll.sh

export PRIVATE_KEY=0xad689d9b7751da07b0fb39c5091672cbfe50f59131db015f8a0e76c9790a6fcc
npx hardhat run --network ganache scripts/deploy.ts
