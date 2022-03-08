import pandas as pd
import numpy as np

try:
    from scipy import sparse
except ImportError:
    print("install scipy for sparse matrix support")


def get_somas_with_types(
    client,
    cell_type_table: str,
    soma_table: str = None,
    split_frame: bool = True,
    root_id_column="pt_root_id",
):
    """returns a dataframe which has a row for every soma in the table
    columns added for n_soma.

    For those with precisely 1 soma in the column, merges in cell type
    information found in the cell type table provided.

    For those somas with more than one, or zero

    Args:
        client (CAVEclient): caveclient initialized with datastack of interest
        cell_type_table (str): name of cell_type table to merge in
        soma_table (str, optional): name of soma table to use. Defaults to soma table specified in client.
        root_id_column (str, optional): what column to use for finding root id (default='pt_root_id')
    Returns:
        soma_df: pd.DataFrame with a row for every soma, a column n_soma has been added to count how
    many soma have the same pt_root_id. For rows with n_soma=1 the cell type table has been merged.
    """
    print("tbd")


def synapse_to_connections(
    syn_df,
    count_synapses: bool = True,
    sum_size: bool = True,
    pre_column: str = "pre_pt_root_id",
    post_column: str = "post_pt_root_id",
):
    """convert a synapse table to a connections table

    quantifies each connection with a common pre and post synaptic partner
    adds columns for number of synapses and summed synapse size (if present in table)

    Args:
        syn_df (_type_): _description_
        count_synapses (bool, optional): _description_. Defaults to True.
        sum_size (bool, optional): _description_. Defaults to True.
        pre_column (str, optional): _description_. Defaults to 'pre_pt_root_id'.
        post_column (str, optional): _description_. Defaults to 'post_pt_root_id'.
    """
    print("tbd")


def merge_cell_types_to_connections(
    conn_df: pd.DataFrame, cell_type_df: pd.DataFrame, remove_multi_calls: bool = True
):
    """_summary_

    Args:
        conn_df (pd.DataFrame): connection dataframe
         built with 'caveclient.analysis.synapse_to_connection'
        cell_type_df (pd.DataFrame): cell_type dataframe
        built with 'caveclient.analysis.get_somas_with_types'
        remove_multi_calls (bool, optional): whether to remove cell type calls for multi-soma. Defaults to True.
    """
    print("tbd")


def make_connection_matrix(
    df: pd.DataFrame,
    quantify: str = "n_synapses",
    return_as: str = "dataframe",
    fill_na: bool = True,
    soma_column_prefix: str = "n_soma",
):
    """convert a synapse or connection dataframe to a connection matrix
    raises a warning if you pass a dataframe without 'n_soma_pre' and 'n_soma_post' as a column
    or if included that there are entries with 'n_soma_pre' or 'n_soma_post' >1
    indicating you might be analyzing a graph with obvious errors.

    Args:
        df (pd.DataFrame): a dataframe containing synapses or connections
        quantify (str, optional): how to quantify connections.
                'n_synapses' counts the number of synapses
                'sum_size' sums the synapse sizes (if size or sum_size) is a column
        return_as (str, optional): how to return the result
                'dataframe' returns this as a dataframe with rows as pre-synaptic-ids
                            and columns as post-synaptic root-ids
                'sparse' returns this as a sparse.csgraph object
        fill_na (bool, optional): whether to fill missing entries with 0s. Defaults to True.
        soma_column_prefix (str, optional): what prefix to find the n_soma columns (_pre/_post).
                                            Defaults to "n_soma".
    """
