import collections, glob, os, re, time
from multiprocessing import Pool

from constants import INPUT_DIR, OUTPUT_DIR

"""
Method to process the generated data of any artefact.
"""
def process_artefact(ARTEFACT_DIR):
    # Determine start time.
    t0 = time.time()

    # Extract the artefact name from the directory.
    artefact = '{}:{}${}'.format(*os.path.basename(ARTEFACT_DIR).split('_'))

    # Extract all the URI and callgraph data.
    uris_content = '\n'.join([ open(file).read() for file in glob.glob(os.path.join(ARTEFACT_DIR, '*', 'uris.csv')) ])
    callgraph_content = '\n'.join([ open(file).read() for file in glob.glob(os.path.join(ARTEFACT_DIR, '*', 'callgraph.csv')) ])

    # Map every ID to an artefact.
    id_to_artefact = { int(id):pkg for id,pkg in re.findall(r'(\d+),"fasten://mvn!([\w\.-]+:[\w\.-]+\$[\w\.-]+)/', uris_content) }

    # Extract the IDs of the artefact we are analysing, also filter based on the exlusion regex.
    artefact_ids = { id for id,uri in id_to_artefact.items() if artefact in uri }

    # Calculate all artefacts that call a given method.
    artefacts_that_call_id = collections.defaultdict(set)
    for source, target in re.findall(r'(\d+),(\d+)', callgraph_content):
        source_id = int(source)
        target_id = int(target)

        # If the method called is part of the artefact we are analysing, add the calling artefact.
        if target_id in artefact_ids:
            artefacts_that_call_id[target_id].add(id_to_artefact[source_id])

    # Sort all IDs based on the number of unique artefacts that call them.
    artefacts_that_call_id = { k:v for k,v in sorted(artefacts_that_call_id.items(), reverse=True, key=lambda x:len(x[1])) }

    # Calculate the total number of uniqe dependents in all URI files.
    num_dependents = float(len({ uri for uri in re.findall(r'fasten://mvn!([\w\.-]+:[\w\.-]+\$[\w\.-]+)/', uris_content) }))

    # Store all the dependent percentages in a file in range [0,1].
    os.makedirs(os.path.join(OUTPUT_DIR, artefact), exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, artefact, 'dependent-percentage.bin'), 'w') as file:
        file.write('\n'.join([ f'{k},{len(v)/num_dependents}' for k,v in artefacts_that_call_id.items() ]))

if __name__ == '__main__':
    # Analyse the artefacts in parallel.
    Pool().map(process_artefact, glob.glob(os.path.join(INPUT_DIR, '*')))
