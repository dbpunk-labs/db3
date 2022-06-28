# Contribution guidelines

First off, thank you for considering contributing to rtstore-tpl.

If your contribution is not straightforward, please first discuss the change you
wish to make by creating a new issue before making the change.

## Reporting issues

Before reporting an issue on the
[issue tracker](https://github.com/rtstore/rtstore/issues),
please check that it has not already been reported by searching for some related
keywords.

## Pull requests

Try to do one pull request per change.

### Updating the changelog

Update the changes you have made in
[CHANGELOG](https://github.com/rtstore/rtstore/blob/main/CHANGELOG.md)
file under the **Unreleased** section.

Add the changes of your pull request to one of the following subsections,
depending on the types of changes defined by
[Keep a changelog](https://keepachangelog.com/en/1.0.0/):

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

If the required subsection does not exist yet under **Unreleased**, create it!

## Developing


### Set up ETCD

please go to https://etcd.io/docs/v3.5/install/


### Set up Object Store

```
sh tools/run_mino.sh
```

### Build and Test

This is no different than other Rust projects.

```shell
git clone https://github.com/rtstore/rtstore
cd rtstore
source tools/init_aws_dev.sh
cargo test
```

