---
title: Contributing
---

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs

Report bugs to our [issues page](https://github.com/{{ config.repo_name }}/issues).

If you are reporting a bug, please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug, in the form of a [minimal reproducible example](https://stackoverflow.com/help/minimal-reproducible-example).

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

`{{ names.package }}` could always use more documentation, whether as part of the
official `{{ names.package }}` docs, in docstrings, or even on the web in blog posts,
articles, and such.

### Submit Feedback

The best way to send feedback is to [create an issue](https://github.com/{{ config.repo_name }}/issues/new) on GitHub.

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that while contributions are welcome, developer/maintainer time is limited.

## Modifying Code

Ready to contribute? Here's how to set up `{{ names.package }}` for local development.

- [Fork](https://github.com/{{ config.repo_name }}/fork) the repo on GitHub.
- Clone your fork locally

```console
git clone git@github.com:your_name_here/{{ names.repo_title }}.git
```

- We use [`uv`](https://docs.astral.sh/uv/) for various developer tasks. Ensure you have `uv` installed according to the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/).

!!! note

    While we recommend using `uv` as described here, these tasks could also be achieved using `pip` to install and run the various required tools. You can view the development requirements and build/check commands in our [`pyproject.toml`](https://github.com/CAVEconnectome/CAVEclient/blob/master/pyproject.toml), so we avoid duplicating them here.

- Navigate to the newly cloned directory, e.g.:

  ```console
  cd {{ names.repo_title }}
  ```

- Create a synced virtual environment, optionally specifying a Python version:

  ```console
  uv sync --python 3.12
  ```

- Create a branch for local development:

  ```console
  git checkout -b name-of-your-bugfix-or-feature
  ```

- Make your changes locally

- If you have added code that should be tested, add [tests](https://github.com/{{ config.repo_name }}/tree/master/tests).

- If you have modified dependencies in any way, make sure to run

  ```console
  uv sync
  ```

- Make sure you have added documentation for any additions or modifications to public functions or classes. You can build the documentation locally to make sure it renders correctly with:

  ```console
  uvx --from poethepoet poe doc-build
  ```

## Automated Checks

- Run the autoformatter:

  ```console
  uvx --from poethepoet poe lint-fix
  ```

- Ensure that your changes pass the checks that will be run on Github Actions, including building the documentation, checking the formatting of the code, and running the tests. To run all at once, do:

  ```console
  uvx --from poethepoet poe checks
  ```

- You may be interested in running some of these checks individually, such as:
    - To run the tests:
  
      ```console
      uvx --from poethepoet poe test
      ```

    - To build the documentation:

      ```console
      uvx --from poethepoet poe doc-build
      ```

    - To run the linter

      ```console
      uvx --from poethepoet poe lint
      ```

## Submitting a Pull Request

- Ensure your code has passed all of the tests described above.
- Commit your changes and push your branch to GitHub:

  ```console
  git add .
  git commit -m "Your detailed description of your changes."
  git push origin name-of-your-bugfix-or-feature
  ```

- [Submit a pull request](https://github.com/{{ config.repo_name }}/compare) through the GitHub website.

Before you submit a pull request, check that it meets these guidelines:

- The pull request should include tests if adding a new feature.
- The docs should be updated with whatever changes you have made. Put
  your new functionality into a function with a docstring, and make sure the new
  functionality is documented after building the documentation (described above).

- Once you submit a pull request, automated checks will run. You may require administrator approval before running these checks if this is your first time contributing to the repo.