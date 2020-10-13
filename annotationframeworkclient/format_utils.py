from urllib.parse import urlparse


def format_precomputed_neuroglancer(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'gs':
        objurl_out = f'precomputed://{objurl}'
    elif qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = f'precomputed://gs://{qry.path[1:]}'
    else:
        objurl_out = None
    return objurl_out


def format_precomputed_https(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'gs':
        objurl_out = f'precomputed://https://storage.googleapis.com/{qry.path[1:]}'
    elif qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = f'precomputed://{objurl}'
    else:
        objurl_out = None
    return objurl_out


def format_graphene(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = f'graphene://{objurl}'
    elif qry.scheme == 'graphene':
        objurl_out = objurl
    else:
        objurl_out = None
    return objurl_out


def format_cloudvolume(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'graphene':
        return format_graphene(objurl)
    elif qry.scheme == 'gs' or qry.scheme == 'http' or qry.scheme == 'https':
        return format_precomputed_https(objurl)
    else:
        return None


def format_raw(objurl):
    return objurl


# No reformatting
output_map_raw = {}

# Use precomputed://gs:// links for neuroglancer, but use precomputed://https://storage.googleapis.com links in cloudvolume
output_map_precomputed = {'raw': format_raw,
                          'cloudvolume': format_precomputed_https,
                          'neuroglancer': format_precomputed_neuroglancer}

# Use graphene://https:// links for both neuroglancer and cloudvolume
output_map_graphene = {'raw': format_raw,
                       'cloudvolume': format_graphene,
                       'neuroglancer': format_graphene}
