import networkx as nx
import glob, os, re, time
from multiprocessing import Pool
import matplotlib.pyplot as plt

from constants import INPUT_DIR, OUTPUT_DIR

"""
Method to process the generated data of any artefact.
"""
def process_artefact(ARTEFACT_DIR):
    # Determine start time.
    t0 = time.time()

    # Extract the artefact name from the directory.
    artefact = '{}:{}${}'.format(*os.path.basename(ARTEFACT_DIR).split('_'))

    # Extract the method calls from the callgraph files.
    callgraph_files = glob.glob(os.path.join(ARTEFACT_DIR, '*', 'callgraph.csv'))
    callgraph_data = '\n'.join([ open(file).read() for file in callgraph_files ]).replace('source,target\n', '').split()

    # Extract the list of all URIs of all methods within the analysed artefact.
    uris_files = glob.glob(os.path.join(ARTEFACT_DIR, '*', 'uris.csv'))
    uris = set().union(*[ open(file).read().split() for file in uris_files ])

    # Extract the IDs of the artefact we are analysing, also filter based on the exlusion regex.
    artefact_ids = { int(uri.split(',')[0]) for uri in uris if artefact in uri }

    # If no artefact IDs have been extracted, return (happens when analysing a purely Scala or Kotlin library).
    if not artefact_ids:
        return

    # Load the callgraph.
    callgraph = nx.read_edgelist(callgraph_data, nodetype=int, delimiter=',')

    # Calculate the degree centrality and store the values of the methods we are analysing.
    degree = { k:v for k,v in nx.degree_centrality(callgraph).items() if k in artefact_ids }
    degree = { k:v for k,v in sorted(degree.items(), key=lambda x:x[1], reverse=True) }

    # Store all the degree values in a file in range [0,1].
    os.makedirs(os.path.join(OUTPUT_DIR, artefact), exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, artefact, 'degree.bin'), 'w') as file:
        max_v = max(degree.values()); min_v = min(degree.values()) 
        file.write('\n'.join([ f'{k},{(v - min_v) / (max_v - min_v)}' for k,v in degree.items() ]))
    
    print(f'Calculated degree centrality in {time.time() - t0:.2f} seconds for {artefact}')

if __name__ == '__main__':
    # Analyse the artefacts in parallel.
    Pool().map(process_artefact, glob.glob(os.path.join(INPUT_DIR, '*')))