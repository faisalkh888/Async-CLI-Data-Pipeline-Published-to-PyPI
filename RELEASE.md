# TestPyPI release

The repository is prepared to publish to TestPyPI when a `v*.*.*` tag is pushed. For
version `0.2.0`, CI runs the test, lint, typing, and build jobs first, then the
`publish-testpypi` job builds and publishes the artifacts.

## One-time setup

1. Push this project to a GitHub repository and add that repository as the local Git
   remote.
2. Create or select the TestPyPI project `async-datapipe-cli`.
3. Configure a TestPyPI trusted publisher for the GitHub owner, repository, workflow
   filename `.github/workflows/ci.yml`, and environment name `testpypi`.
4. Create the matching GitHub Actions environment named `testpypi`.

Do not store a PyPI API token in the repository. The workflow uses GitHub's OIDC trusted
publishing integration.

## Publish a version

Update `version` in `pyproject.toml`, run `uv lock`, then run `uv sync --locked --extra
dev`, `uv run pytest`, `uv run ruff check .`, `uv run mypy src`, and `uv build`. Commit
those changes, create a matching version tag such as `v0.2.0`, and push the commit and
tag. The tag should match the package version.

After the TestPyPI upload succeeds, install into a clean environment and verify the CLI:

```sh
python -m pip install --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple async-datapipe-cli
datapipe --version
```
