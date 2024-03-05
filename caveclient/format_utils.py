from urllib.parse import urlparse


def format_precomputed_neuroglancer(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "gs":
        objurl_out = f"precomputed://{objurl}"
    elif qry.scheme == "http" or qry.scheme == "https":
        objurl_out = f"precomputed://gs://{qry.path[1:]}"
    else:
        objurl_out = None
    return objurl_out


def format_neuroglancer(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "graphene" or "https":
        return format_graphene(objurl)
    elif qry.scheme == "precomputed":
        return format_precomputed_neuroglancer(objurl)
    else:
        return format_raw(objurl)


def format_precomputed_https(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "gs":
        objurl_out = f"precomputed://https://storage.googleapis.com/{qry.path[1:]}"
    elif qry.scheme == "http" or qry.scheme == "https":
        objurl_out = f"precomputed://{objurl}"
    else:
        objurl_out = None
    return objurl_out


def format_graphene(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "http" or qry.scheme == "https":
        objurl_out = f"graphene://{objurl}"
    elif qry.scheme == "graphene":
        objurl_out = objurl
    else:
        objurl_out = None
    return objurl_out


def format_verbose_graphene(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "http" or qry.scheme == "https":
        objurl_out = f"graphene://middleauth+{objurl}"
    elif qry.scheme == "graphene":
        objurl_out = f"graphene://middleauth+{qry.netloc}{qry.path}"
    return objurl_out


def format_cloudvolume(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "graphene":
        return format_graphene(objurl)
    elif qry.scheme == "gs" or qry.scheme == "http" or qry.scheme == "https":
        return format_precomputed_https(objurl)
    else:
        return None


def format_raw(objurl):
    return objurl


def format_cave_explorer(objurl):
    qry = urlparse(objurl)
    if qry.scheme == "graphene" or qry.scheme == "https":
        return format_verbose_graphene(objurl)
    elif qry.scheme == "precomputed":
        return format_precomputed_neuroglancer(objurl)
    else:
        return None


# Use graphene://https:// links for both neuroglancer and cloudvolume

output_map = {
    "raw": format_raw,
    "cloudvolume": format_cloudvolume,
    "neuroglancer": format_neuroglancer,
    "cave_explorer": format_cave_explorer,
    "cave-explorer": format_cave_explorer,
}
