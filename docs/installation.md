---
title: Installation
---

## Stable release

To install `{{ names.package }}`, run this command in your
terminal:

```console
pip install {{ names.package }}
```

This is the preferred method to install `{{ names.package }}`, as it will always
install the most recent stable release.

You can also specify a particular version, e.g.

```console
pip install {{ names.package }}==5.0.0
```

If you don't have [pip][] installed, this [Python installation guide][]
can guide you through the process.

## From source

The source for `{{ names.package }}` can be downloaded from
the [Github repo][].

You can either clone the public repository:

```console
git clone git://github.com/{{ config.repo_name }}
```

Or download the [tarball][]:

```console
curl -OJL https://github.com/{{ config.repo_name }}/tarball/{{ names.main_branch }}
```

Once you have a copy of the source, you can install it with:

```console
pip install .
```

Or in editable mode, it can be installed with:

```console
pip install -e .
```

[pip]: https://pip.pypa.io
[Python installation guide]: http://docs.python-guide.org/en/latest/starting/installation/

[Github repo]: https://github.com/{{ config.repo_name }}
[tarball]: https://github.com/{{ config.repo_name }}/tarball/{{ names.main_branch }}
