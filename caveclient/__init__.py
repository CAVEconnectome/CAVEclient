__version__ = "4.20.1"

from .frameworkclient import CAVEclient

try:
    import requests
    import json
    from packaging import version
    import warnings

    r = requests.get("https://pypi.org/pypi/caveclient/json")
    pypi_version = json.loads(r.content)["info"]["version"]

    if version.parse(pypi_version) > version.parse(__version__):
        warnings.warn(f"New CAVEclient version available! Your version: {__version__} Latest version: {pypi_version}.", UserWarning)
except:
    pass