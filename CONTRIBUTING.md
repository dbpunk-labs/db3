# Contribution guidelines

First off, thank you for considering contributing to db3.

If your contribution is not straightforward, please first discuss the change you
wish to make by creating a new issue before making the change.

## Reporting issues

Before reporting an issue on the
[issue tracker](https://github.com/dbpunk-labs/db3/issues),
please check that it has not already been reported by searching for some related
keywords.

## Pull requests

Try to do one pull request per change.

### Updating the changelog

Update the changes you have made in
[CHANGELOG](https://github.com/dbpunk-labs/db3/blob/main/CHANGELOG.md)
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

### Build and Test

```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 & bash install_env.sh && cargo build
cargo test
```

## Update Documents

if you want update db3 documents , you can follow the steps

### Install Mkdocs

```shell
pip install mkdocs
```
### Document Template

db3 uses https://squidfunk.github.io/mkdocs-material/ as its document framework and you can get started from [here](https://squidfunk.github.io/mkdocs-material/getting-started/)

### Serve the docs

```shell
git clone https://github.com/dbpunk-labs/db3.git
mkdocs serve
```

