import time
import psutil
import yaml

"""Forcer un type de jointure particulier : """

#PostGreSQL :
def postgresql_force_hash_join(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET enable_mergejoin = OFF;")
        cursor.execute("SET enable_nestloop = OFF;")
        cursor.execute("SET enable_hashjoin = ON;")

def postgresql_force_nested_loop(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET enable_hashjoin = OFF;")
        cursor.execute("SET enable_mergejoin = OFF;")
        cursor.execute("SET enable_nestloop = ON;")

def postgresql_reset_parameters(conn):
    with conn.cursor() as cursor:
        cursor.execute("RESET enable_hashjoin;")
        cursor.execute("RESET enable_mergejoin;")
        cursor.execute("RESET enable_nestloop;")
        
#MonetDB
def monetdb_default_fast(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET optimizer='default_fast';")

def monetdb_no_mitosis_pipe_optimizer(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET optimizer='no_mitosis_pipe'")

def monetdb_sequential_pipe_optimizer(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET optimizer='no_mitosis_pipe'")

def monetdb_reset_parameters(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET optimizer='default_pipe';")


"""Requêtes et mesure des performances : """


"""
Mesure le temps d'exécution et la mémoire utilisée par une fonction exécutant une requête SQL.
"""
def measure_metrics(query_function, *args):
    process = psutil.Process()
    t0 = time.time()
    try:
        query_function(*args)
    except Exception as e:
        print(f"Erreur : {e}")
    t1 = time.time() - t0
    return t1


def execute_postgresql_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        cursor.fetchall()

def execute_monetdb_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        cursor.fetchall()
        
        
"""Charger les configs et extraire les requêtes """

def get_max_memory(config):
    with open(config, 'r') as file:
        data = yaml.safe_load(file)
    return config.get('maximum_memory')

def extract_queries_from_yaml(config):
    with open(config, 'r') as file:
        data = yaml.safe_load(file)
    queries = [query for query in data['queries'].values()]
    return queries

def load_monetdb_config(config_file):
    try:
        with open(config_file, encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config.get('monetdb')
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier de configuration : {e}")
        return None
    
def load_postgresql_config(config_file):
    try:
        with open(config_file, "r",encoding="utf-8") as file:
            config = yaml.safe_load(file)
        return config["postgresql"]
    except Exception as e:
        print(f"Erreur lors du chargement du fichier de configuration : {e}")
        exit(1)
