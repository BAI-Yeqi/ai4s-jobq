# Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
<https://cla.microsoft.com>.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (for example, label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Install development dependencies

```bash
pip install -e '.[dev]'
npm install
```

## Run tests

Tests use Azurite to emulate Azure Blob Storage and Azure Queue Storage. Start both
services before running the test suite:

```bash
./node_modules/.bin/azurite-blob \
  --skipApiVersionCheck --inMemoryPersistence --disableTelemetry --blobPort 10000 &
./node_modules/.bin/azurite-queue \
  --skipApiVersionCheck --inMemoryPersistence --disableTelemetry --queuePort 10001 &
pytest
```

CI runs the test suite directly with `pytest` on Python 3.10, 3.11, and 3.12. The
repository still contains a `tox.ini` configuration for running the same version
matrix locally, but tox is not used in CI or required for the standard development
workflow.

## Release

Releases are managed by maintainers through GitHub Actions:

1. Update `CHANGELOG.md` with the version and release date, then merge the change
   after CI passes.
2. Create and push a tag named `v<version>`. The version is derived from the tag by
   `setuptools-scm`.
3. The tag triggers the workflows that build the distributions and publish them to
   the configured Azure Artifacts feeds.

The TestPyPI workflow can be run manually to validate a distribution before release.
Publishing to public PyPI is disabled in this repository.

Documentation is built and deployed to GitHub Pages automatically after changes are
merged to `main`; no `gh-pages` branch or manual `make release-docs` step is required.
