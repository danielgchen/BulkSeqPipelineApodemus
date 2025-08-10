# cellphone db for ligand receptor
CELLPHONEDB = "/home/dchen2/PACKAGES/cellphonedb-data-5.0.0"
# databases and annotations for pyscenic
TF_DB = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/pyscenic_data/allTFs_hg38.txt"
FEATHER_PAT = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/pyscenic_data/*.feather"
ANNO_FN = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/pyscenic_data/motifs-v10nr_clust-nr.hgnc-m0.001-o0.0.tbl"
# metaflux annotations
GEM_FILE = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/metaflux/human_gem.csv"
# kinase database
K3_URL = 'https://amp.pharm.mssm.edu/kea3/api/enrich/'
# chea database
C3_URL = 'https://maayanlab.cloud/chea3/api/enrich/'
# enrichr
UPLOAD_URL = 'https://maayanlab.cloud/Enrichr/addList'
ENRICH_URL = 'https://maayanlab.cloud/Enrichr/enrich'
ENRICH_QUERY = '?userListId=%s&backgroundType=%s'
ENRICH_LIBRARIES = [
    'KEGG_2021_Human',
    'Reactome_Pathways_2024',
    'WikiPathways_2024_Human',
    'Elsevier_Pathway_Collection',
    'BioPlanet_2019',
    'MSigDB_Hallmark_2020',
    'GO_Biological_Process_2025',
    'GO_Cellular_Component_2025',
    'GO_Molecular_Function_2025'
]