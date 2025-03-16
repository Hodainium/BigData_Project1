from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import csv


cluster = Cluster() #Cluster(['192.168.0.1', '192.168.0.2'])
session = cluster.connect()

create_keyspace_query = """
CREATE KEYSPACE IF NOT EXISTS diseases 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"""

session.execute(create_keyspace_query)
print("Keyspace 'diseases' created successfully!")

session.set_keyspace('diseases')
print("Keyspace 'diseases' set successfully!")


def load_nodes(file_path):
    nodes = {}
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            nodes[row['id']] = {'name': row['name'], 'kind': row['kind']}
    return nodes


def load_edges(file_path):
    edges = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            edges.append({'source': row['source'], 'metaedge': row['metaedge'], 'target': row['target']})
    return edges


nodes = load_nodes('nodes.tsv')
edges = load_edges('edges.tsv')


def process_disease_info(disease_id):

    disease_name = nodes.get(disease_id, {}).get('name', '')


    drug_names = set()
    gene_names = set()
    locations = set()
    

    for edge in edges:

        if disease_id == edge['target'] and edge['metaedge'] in ['CtD', 'CpD']:
            drug_names.add(nodes.get(edge['source'], {}).get('name', ''))


        elif disease_id == edge['source'] and edge['metaedge'] == 'DaG':
            gene_names.add(nodes.get(edge['target'], {}).get('name', ''))


        elif disease_id == edge['source'] and edge['metaedge'] == 'DlA':
            locations.add(nodes.get(edge['target'], {}).get('name', ''))


    drug_names = list(drug_names)
    gene_names = list(gene_names)
    locations = list(locations)


    query = """
    INSERT INTO disease_info (disease_id, disease_name, drug_names, gene_names, locations)
    VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (disease_id, disease_name, drug_names, gene_names, locations))

def query_disease_locations(session, disease_id):
    disease_id_with_prefix = f"Disease::DOID:{disease_id}"

    query = f"SELECT * FROM disease_info WHERE disease_id = '{disease_id_with_prefix}'"
    rows = session.execute(query)

    print("\n" + "Query results for: " + disease_id + "\n")

    if not rows:
        print("No results found. \n")
    else:   
        for row in rows:            
            print("Disease name: " + row.disease_name + "\n")
            print("Drug names")
            print(row.drug_names, "\n")
            print("Gene names")
            print(row.gene_names, "\n")
            print("Anatomy locations")
            print(row.locations, "\n")
        


create_table_query = """
CREATE TABLE IF NOT EXISTS disease_info (
    disease_id TEXT PRIMARY KEY,
    disease_name TEXT,
    drug_names LIST<TEXT>,
    gene_names LIST<TEXT>,
    locations LIST<TEXT>
);
"""

session.execute(create_table_query)

for disease_id in nodes:
    if nodes[disease_id]['kind'] == 'Disease':  
        process_disease_info(disease_id)

print("Data processing completed! \n")

while True:
    disease_id = input("Please enter the disease ID: ")
    query_disease_locations(session, disease_id)

#session.shutdown()
