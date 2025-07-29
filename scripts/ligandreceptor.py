import argparse
import logging
import os
import pandas as pd
from typing import Dict, Tuple

# create a logger object writing to the given file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def retrieve_cellphonedb(cellphonedb_directory: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
    logger.info(f"Reading in CellPhoneDB parameters from {cellphonedb_directory}")
    # read in cellphonedb
    df_intrxn = pd.read_csv(os.path.join(cellphonedb_directory, 'interaction_table.csv'))
    df_gene = pd.read_csv(os.path.join(cellphonedb_directory, 'gene_table.csv'))
    df_complex = pd.read_csv(os.path.join(cellphonedb_directory, 'complex_composition_table.csv'))
    pid2gn = df_gene[['protein_id','gene_name']].value_counts().reset_index().set_index('protein_id')['gene_name']
    df_complex['gene_name'] = pid2gn.loc[df_complex['protein_multidata_id']].values
    return df_intrxn, df_gene, df_complex, pid2gn

def filter_cellphonedb(df_gene: pd.DataFrame, df_complex: pd.DataFrame, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Filtering CellPhoneDB based on the expression dataframe")
    # TODO: add a human to mouse mapping
    # retrieve gene expression
    genes = df.index.intersection(df_gene['gene_name'].unique())
    df_gex = df.loc[genes].copy().T
    # only keep those with equivalents
    idxs_complexs = df_complex.loc[df_complex['gene_name'].isna(), 'complex_multidata_id']
    df_complex = df_complex.loc[~df_complex['complex_multidata_id'].isin(idxs_complexs)]
    idxs_complexs = df_complex.loc[~df_complex['gene_name'].isin(df_gex.columns), 'complex_multidata_id']
    df_complex = df_complex.loc[~df_complex['complex_multidata_id'].isin(idxs_complexs)]
    df_gene = df_gene.dropna()
    df_gene = df_gene.loc[df_gene['gene_name'].isin(df_gex.columns)]
    # TODO: auto-filter interactions here
    return df_gene, df_complex

def calculate_expression(df_gene: pd.DataFrame, df_complex: pd.DataFrame, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    # compile all of the profiles
    df_profiles = []
    c2gns = {}
    # compile complexes
    for complex in df_complex['complex_multidata_id'].unique():
        mask = df_complex['complex_multidata_id'] == complex
        c2gns[complex] = '-'.join(df_complex.loc[mask, 'gene_name'].tolist())
        df_profile = df.loc[df_complex.loc[mask, 'gene_name']].mean(0)
        df_profile.name = complex
        df_profiles.append(df_profile)
    # compile individual genes
    pid2gene = df_gene[['protein_id','gene_name']].value_counts().reset_index().set_index('protein_id')['gene_name']
    df_profile = df[pid2gene].copy()
    df_profile.columns = pid2gene.index
    df_profile = pd.concat(df_profiles+[df_profile], axis=1)
    # compute extended mappping of id to gene or complex
    p2g = pid2gene.to_dict()
    p2g.update(c2gns)
    return df_profile, p2g

def score_interactions(df_intrxn: pd.DataFrame, df_profile: pd.DataFrame, p2g: Dict) -> pd.DataFrame:
    # compute associations between each ligand-receptor pair
    scores = []
    for idx in df_intrxn.index:
        a1, a2 = df_intrxn.loc[idx, ['multidata_1_id','multidata_2_id']]
        if a1 not in df_profile.columns or a2 not in df_profile.columns: continue
        score = df_profile[a1] * df_profile[a2]
        score.name = f'{idx}:{a1}<>{a2}'
        scores.append(score)
    df_scores = pd.concat(scores, axis=1).T
    # convert ids to human readable names
    ids = df_scores.index.to_series().str.split(':', expand=True)[1].str.split('<>', expand=True)
    ids = [p2g[int(i1)]+':'+p2g[int(i2)] for i1, i2 in zip(ids[0], ids[1])]
    df_scores.index = ids
    return df_scores

def main():
    # read in command line arguments
    parser = argparse.ArgumentParser(description="CellPhoneDB Autocrine Signaling Quantification")
    parser.add_argument(
        "-c",
        "--cellphone_db",
        type=str,
        default="/home/dchen2/PACKAGES/cellphonedb-data-5.0.0",
        help="Path to CellPhoneDB Database",
    )
    parser.add_argument(
        "-d",
        "--expression_file",
        type=str,
        default="/home/dchen2/TMP/expr.csv",
        help="Path to the expression matrix (rows are genes, columns are samples), or use the transpose option",
    )
    parser.add_argument(
        "-o",
        "--output_file",
        type=str,
        default="/home/dchen2/TMP/expr.ligandreceptor.csv",
        help="Path to the output ligand-receptor matrix",
    )
    parser.add_argument(
        "-t",
        "--transpose",
        type=bool,
        default=False,
        help="Whether to transpose the expression matrix, i.e. if it is rows are columns and genes are samples"
    )
    args = parser.parse_args()

    # gather raw database information
    df_intrxn, df_gene, df_complex, pid2gn = retrieve_cellphonedb(cellphonedb_directory=args.cellphone_db)
    
    # intake the expression dataframe
    df = pd.read_csv(args.expression_file, index_col=0).fillna(0).astype(float)
    if args.transpose:
        df = df.T

    # filter database based on expression information
    df_gene, df_complex = filter_cellphonedb(df_gene=df_gene, df_complex=df_complex, df=df)

    # calculate expression of each gene and complex
    df_profile, p2g = calculate_expression(df_gene=df_gene, df_complex=df_complex, df=df.T)

    # score all ligand-receptor interactions (autocrine manner)
    df_scores = score_interactions(df_intrxn=df_intrxn, df_profile=df_profile, p2g=p2g)

    # write the data
    directory = os.path.dirname(args.output_file)
    os.makedirs(directory, exist_ok=True)
    df_scores.to_csv(args.output_file)


if __name__ == "__main__":
    main()