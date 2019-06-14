import tqdm
import pandas as pd
import numpy as np
import cloudvolume
import datetime
import re

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from annotationframeworkclient.chunkedgraph import ChunkedGraphClient
from emannotationschemas import models as em_models

from multiwrapper import multiprocessing_utils as mu


def lookup_supervoxels(xyzs, server_address=None, pcg_client=None, segmentation_scaling=[2,2,1]):
    '''
    Lookup supervoxel ids from a np array of points
    '''
    if server_address is None and pcg_client is not None:
        server_address = pcg_client.cloudvolume_path

    sv_ids = []
    xyzs = xyzs / np.array(segmentation_scaling)
    cv = cloudvolume.CloudVolumeFactory(cloudurl=server_address,
                                        map_gs_to_https=True,
                                        progress=False)
    for xyz in xyzs:
        sv = cv._cv[xyz[0], xyz[1], xyz[2]]
        sv_ids.append(int(sv.flatten()))
    return sv_ids


def get_materialization_timestamp(materialization_version, sql_database_uri):
    '''
    Query the database for a materialization version and get the utc time stamp.
    '''
    Session = sessionmaker()
    engine = create_engine(sql_database_uri)
    Session.configure(bind=engine)
    session = Session()

    query = session.query(em_models.AnalysisVersion).filter(em_models.AnalysisVersion.version==materialization_version)
    materialization_dt = query.value(column='time_stamp')
    session.close()

    materialization_time_utc = materialization_dt.replace(tzinfo=datetime.timezone.utc).timestamp()
    return materialization_time_utc


def lookup_root_ids(supervoxel_ids, pcg_client):
    '''
    Look up root ids from a list of supervoxels
    '''
    root_ids = []
    for sv_id in supervoxel_ids:
        try:
            oid = pcg_client.get_root_id(sv_id)
            root_ids.append(int(oid))
        except:
            root_ids.append(0)
    return root_ids

def ersatz_point_query_multi(xyzs,
                             pcg_client,
                             segmentation_scaling=[2,2,1],
                             additional_columns={},
                             n_threads=None):
    pt_dict = {}
    for key, xyz in xyzs.items():
        args = []
        for pt in xyz:
            args.append([pt, pcg_client._server_address, pcg_client.table_name, segmentation_scaling])
        lookup_ids = mu.multithread_func(_single_point_query, args, verbose=True, n_threads=n_threads)
        sv_ids = [ids[0] for ids in lookup_ids]
        root_ids = [ids[1] for ids in lookup_ids]
        
        pt_dict['{}_position'.format(key)] = [list(loc) for loc in xyz]
        pt_dict['{}_supervoxel_id'.format(key)] = sv_ids
        pt_dict['{}_root_id'.format(key)] = root_ids

    dat_dict = {**pt_dict, **additional_columns}
    df = pd.DataFrame(dat_dict)
    for key in xyzs.keys():
        df['{}_position'.format(key)] = df['{}_position'.format(key)].astype('O')
        df['{}_supervoxel_id'.format(key)] = df['{}_supervoxel_id'.format(key)].astype('O')
        df['{}_root_id'.format(key)] = df['{}_root_id'.format(key)].astype('O')
    return df


def _single_point_query(args):
    xyz, server_address, table_name, segmentation_scaling = args
    pcg_client = ChunkedGraphClient(server_address=server_address, table_name=table_name)
    sv_id = lookup_supervoxels([xyz], pcg_client=pcg_client, segmentation_scaling=segmentation_scaling)
    root_id = lookup_root_ids(sv_id, pcg_client)
    return (sv_id[0], root_id[0])


def ersatz_point_query(xyzs,
                       pcg_client,
                       segmentation_scaling=[2,2,1],
                       additional_columns={}):
    '''
    Given a set of points, returns a dataframe formatted like a database query.
    Aligned to a particular materialization version.
    :param xyzs: dict of Nx3 array of point positions in supervoxels. The key is the prefix to give the 
    :param materialization_version: Int, version in the materialized database.
    :param sql_database_uri: String, materialization database URI.
    :param cv_path: String, cloudvolume path.
    :param segmentation_scaling: 3 element array, Gives xyz scaling between segmentation and imagery for CloudVolume
    :param additional_columns: Dict with keys as strings and N-length array-likes as values. Extra columns in dataframe.
    '''
    pt_dict = {}
    for key, xyz in xyzs.items():
        sv_ids = lookup_supervoxels(xyz, pcg_client=pcg_client, segmentation_scaling=segmentation_scaling)
        root_ids = lookup_root_ids(sv_ids, pcg_client)

        pt_dict['{}_position'.format(key)] = [list(loc) for loc in xyz]
        pt_dict['{}_supervoxel_id'.format(key)] = sv_ids
        pt_dict['{}_root_id'.format(key)] = root_ids

    dat_dict = {**pt_dict, **additional_columns}
    df = pd.DataFrame(dat_dict)
    for key in xyzs.keys():
        df['{}_position'.format(key)] = df['{}_position'.format(key)].astype('O')
        df['{}_supervoxel_id'.format(key)] = df['{}_supervoxel_id'.format(key)].astype('O')
        df['{}_root_id'.format(key)] = df['{}_root_id'.format(key)].astype('O')
    return df

def ersatz_annotation_query(annos,
                            pcg_client,
                            bound_point_keys=['pt'],
                            segmentation_scaling=[2,2,1],
                            add_id=True,
                            omit_type=True,
                            n_threads=1):
    xyzs = {}
    additional_columns = {}

    for key in annos[0].keys():
        if omit_type is True and key is 'type':
            continue

        if key in bound_point_keys:
            xyzs[key] = []
        else:
            additional_columns[key] = []

    for anno in annos:
        for key in xyzs.keys():
            xyzs[key].append(anno[key]['position'])
        for key in additional_columns.keys():
            if omit_type is True and key is 'type':
                continue
            if type(anno[key]) is not dict:
                additional_columns[key].append(anno[key])
            else:
                additional_columns[key].append(anno[key]['position'])

    for key in xyzs:
        xyzs[key] = np.array(xyzs[key])
    
    if 'id' not in annos[0].keys() and add_id is True:
        additional_columns['id'] = np.arange(1,len(annos)+1)

    if n_threads == 1:
        df =  ersatz_point_query(xyzs,
                                 pcg_client=pcg_client,
                                 segmentation_scaling=segmentation_scaling,
                                 additional_columns=additional_columns)
    else:
        df =  ersatz_point_query_multi(xyzs,
                         pcg_client=pcg_client,
                         segmentation_scaling=segmentation_scaling,
                         additional_columns=additional_columns,
                         n_threads=n_threads)

    if add_id is True:
        df = df.reindex(['id'] + list(df.columns[df.columns!='id']), axis=1)
    return df
