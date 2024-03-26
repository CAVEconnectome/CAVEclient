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

## Get Started

Ready to contribute? Here's how to set up `{{ names.package }}` for local development.

- [Fork](https://github.com/{{ config.repo_name }}/fork) the repo on GitHub.
- Clone your fork locally

```console
git clone git@github.com:your_name_here/{{ names.repo_title }}.git
```

- Ensure [pip](https://pip.pypa.io/en/stable/installation/) is installed.
- Create a virtual environment (here we use venv):

  ```console
  python3 -m venv .venv
  ```

- Start your virtualenv:

  ```console
  source .venv/bin/activate
  ```

- Create a branch for local development:

  ```console
  git checkout -b name-of-your-bugfix-or-feature
  ```

- Make your changes locally
- Install development requirements:

  ```console
  pip install -r test_requirements.txt
  pip install -e .
  ```

- When you're done making changes, check that your changes pass the
  tests by running [pytest](https://docs.pytest.org/en/):

  ```console
  pytest tests
  ```

  Note that once you submit your pull request, GitHub Actions will run the tests also,
  including on multiple operating systems and Python versions. Your pull request will
  have to pass on all of these before it can be merged.

- Ensure your contribution meets style guidelines. First, install [ruff](https://docs.astral.sh/ruff/):

  ```console
  pip install ruff
  ```

- Fix linting and formatting. From the root of the repository, run the following commands:

  ```console
  ruff check . --extend-select I --fix
  ruff format .
  ```

- Commit your changes and push your branch to GitHub:

  ```console
  git add .
  git commit -m "Your detailed description of your changes."
  git push origin name-of-your-bugfix-or-feature
  ```

- [Submit a pull request](https://github.com/{{ config.repo_name }}/compare) through the GitHub website.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

- The pull request should include tests if adding a new feature.
- The docs should be updated with whatever changes you have made. Put
  your new functionality into a function with a docstring, and make sure the new
  functionality is documented after building the documentation.

## Documentation style

We use [mkdocs](https://www.mkdocs.org/) to build the documentation. In particular, we
use the [mkdocs-material](https://squidfunk.github.io/mkdocs-material/) theme, and a
variety of other extensions.

!!! note

    More information codifying our documentation style and principles coming soon. For
    now, just try to follow the style of the existing documentation.
