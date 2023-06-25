#! /bin/sh
#
# format.sh


yarn prettier --write src tests ./jest.setup.ts ./jest.config.ts
