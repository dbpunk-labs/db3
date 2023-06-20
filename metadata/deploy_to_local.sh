#! /bin/sh
#
# deploy_to_scoll.sh

export PRIVATE_KEY=0xe3cd3444d4c05cb2bb39fceb578c27a92ab457d3dfb2b209015867a75ecb9d3c
npx hardhat run --network mumbai scripts/deploy.ts
