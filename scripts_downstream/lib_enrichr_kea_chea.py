import json
import pandas as pd
import requests
import time
from typing import Dict, List, Tuple
# private packages
from constants import *


def run_url(gene_set: List[str], query_name: str, url: str):
    # define payload
    payload = {
        'gene_set': gene_set,
        'query_name': query_name
    }
    # retrieve response for up to five attempts
    n_trials = 0
    while n_trials < 5:
        response = requests.post(url, data=json.dumps(payload))
        # exit if possible
        if response.ok:
            return json.loads(response.text)
        # move up a trial run sleeping for a minute
        n_trials += 1
        time.sleep(60)
    raise ValueError(f"Query could not run through for {url}")

def process_kea3(results: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # retrieve average and top ranks
    data_mean = pd.DataFrame(results['Integrated--meanRank'])
    data_mean['Integrated'] = 'meanRank'
    data_top = pd.DataFrame(results['Integrated--topRank'])
    data_top['Integrated'] = 'topRank'
    data_int = pd.concat([data_mean, data_top], axis=0)
    # sort by score as float
    data_int['Score'] = data_int['Score'].astype(float)
    data_int = data_int.sort_values('Score')
    # create a tracker variable for individual libraries
    datas = []
    # work through individual libraries and concatenate
    keys = ['PTMsigDB','prePPI','mentha','The_Kinase_Library','MINT','STRING','ChengKSIN','HIPPIE','BioGRID','PhosDAll','ChengPPI','STRING.bind']
    for key in keys:
        datas.append(pd.DataFrame(results[key]))
    data_ind = pd.concat(datas, axis=0)
    # sort by FDR as float
    data_ind['FDR'] = data_ind['FDR'].astype(float)
    data_ind = data_ind.sort_values('FDR')[::-1]
    return data_int, data_ind

def process_chea3(results: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # retrieve average and top ranks
    data_mean = pd.DataFrame(results['Integrated--meanRank'])
    data_mean['Integrated'] = 'meanRank'
    data_top = pd.DataFrame(results['Integrated--topRank'])
    data_top['Integrated'] = 'topRank'
    data_int = pd.concat([data_mean, data_top], axis=0)
    # sort by score as float
    data_int['Score'] = data_int['Score'].astype(float)
    data_int = data_int.sort_values('Score')
    # create a tracker variable for individual libraries
    datas = []
    # work through individual libraries and concatenate
    keys = ['GTEx--Coexpression', 'ReMap--ChIP-seq', 'Enrichr--Queries', 'ENCODE--ChIP-seq', 'ARCHS4--Coexpression', 'Literature--ChIP-seq']
    for key in keys:
        datas.append(pd.DataFrame(results[key]))
    data_ind = pd.concat(datas, axis=0)
    # sort by FDR as float
    data_ind['FDR'] = data_ind['FDR'].astype(float)
    data_ind = data_ind.sort_values('FDR')[::-1]
    return data_int, data_ind


def run_enrichr(gene_set: List[str], query_name: str, out_fn: str) -> None:
    # load up the payload with the requested gene list
    payload = {
        'list': (None, gene_set),
        'description': (None, query_name)
    }
    # push the response up to the gene list
    response = requests.post(UPLOAD_URL, files=payload)
    if not response.ok:
        raise Exception('Error analyzing gene list')
    # retrieve the user list id
    data = json.loads(response.text)
    user_list_id = data['userListId']
    # loop through each library of interest
    result_dataframes = {}
    for gene_set_library in ENRICH_LIBRARIES:
        # get the enrichment response
        response = requests.get(
            ENRICH_URL + ENRICH_QUERY % (user_list_id, gene_set_library)
         )
        if not response.ok:
            raise Exception('Error fetching enrichment results')
        # retrieve results for this library
        result = json.loads(response.text)
        columns = ['ID','Name','P-value','Odds Ratio','Combined score','Genes','Adjusted p-value','X1','X2']
        result_dataframe = pd.DataFrame(result[gene_set_library], columns=columns)
        result_dataframes[gene_set_library]
    return result_dataframes