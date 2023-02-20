#! /bin/sh
#
# deploy_to_scoll.sh

export PRIVATE_KEY=57c9180841f22d653004f548fbb85af55580cce0b360044723cc3c9d308bbea8
npx hardhat run --network ganache scripts/deploy.ts
