# CAVEclient

![PyPI - Version](https://img.shields.io/pypi/v/CAVEclient)
[![build status](https://github.com/CAVEconnectome/CAVEclient/actions/workflows/daily.yml/badge.svg)](https://github.com/CAVEconnectome/CAVEclient/actions/workflows/daily.yml) [![Downloads](https://static.pepy.tech/badge/caveclient)](https://pepy.tech/project/caveclient)
[![codecov](https://codecov.io/gh/CAVEconnectome/CAVEclient/graph/badge.svg?token=KVI1AG6B8A)](https://codecov.io/gh/CAVEconnectome/CAVEclient)

CAVE is short for Connectome Annotation Versioning Engine. CAVE is a set of microservices
that provide a framework for storing and versioning connectomics data and large sets of
dynamic annotations, metadata, and segmentations. This repository supplies client-side
code to easily interact with the microservices in CAVE.

A full description of the Connectome Annotation Versioning Engine can be found [in this paper](https://www.biorxiv.org/content/10.1101/2023.07.26.550598v1).

## Installation

`CAVEclient` can be installed from PyPI:

```bash
pip install caveclient
```

To add optional dependencies (currently for interfacing with the segmentation, imagery,
and some skeleton formats via cloud-volume), you can install with the following:

```bash
pip install caveclient[cv]
```

## Python version support

Currently we are officially supporting and testing against Python 3.9, 3.10, 3.11 and 3.12.

## Documentation

You can find full documentation at [caveconnectome.github.io/CAVEclient](https://caveconnectome.github.io/CAVEclient).

## Issues

We welcome bug reports and questions. Please post an informative issue on the [GitHub issue tracker](https://github.com/CAVEconnectome/CAVEclient/issues).

## Development

To view information about developing CAVEclient, see our [contributing guide](https://caveconnectome.github.io/CAVEclient/contributing).
