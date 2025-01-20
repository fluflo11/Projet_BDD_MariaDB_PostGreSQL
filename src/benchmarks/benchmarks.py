import pymonetdb
import psycopg2
import time
import psutil
import utilsBenchmark as utils

def append_results_to_file(file_path, content):
    with open(file_path, "a", encoding="utf-8") as file:
        file.write(content + "\n")

def main():
    print("Début benchmark")
    config_file = "config.yaml"
    results_file = "src/benchmarks/results.txt"
    monetdb_config = utils.load_monetdb_config(config_file)
    print("Config MonetDB importée correctement")
    postgresql_config = utils.load_postgresql_config(config_file)
    print("Config PostgreSQL importée correctement")
    queries = utils.extract_queries_from_yaml(config_file)
    print("Requêtes importées correctement")

    """MONETDB"""
    try:
        print("Début benchmark MonetDB")
        connmonetdb = pymonetdb.connect(
            hostname=monetdb_config["host"],
            port=monetdb_config["port"],
            username=monetdb_config["user"],
            password=monetdb_config["password"],
            database=monetdb_config["dbname"]
        )
        print("Connexion à MonetDB réussie.")

        """Requêtes MonetDB + mesures"""
        optimizations = [
            ("default", utils.monetdb_reset_parameters),
            ("default fast", utils.monetdb_default_fast),
            ("no mitosis pipe", utils.monetdb_no_mitosis_pipe_optimizer),
            ("sequential pipe", utils.monetdb_sequential_pipe_optimizer)
        ]

        for opt_name, opt_function in optimizations:
            opt_function(connmonetdb)
            for query in queries:
                elapsed_time = utils.measure_metrics(utils.execute_monetdb_query, connmonetdb, query)
                print(f"MonetDB - Requête :\n {query}")
                print(f"Temps d'exécution : {elapsed_time:.4f} s")

                result = (
                    f"BDD : MonetDB\n"
                    f"Requête : {query.splitlines()[0]}\n"
                    f"Type d'optimisation : {opt_name}\n"
                    f"Temps d'exécution : {elapsed_time:.4f} s\n"
                    f"------------\n"
                )
                append_results_to_file(results_file, result)

        connmonetdb.close()
        print("Connexion à MonetDB fermée.")

    except Exception as e:
        print(f"Erreur lors de la connexion à MonetDB : {e}")

    """POSTGRESQL"""
    try:
        print("Début benchmark PostgreSQL")
        connpostgre = psycopg2.connect(
            options="-c client_encoding=utf-8",
            dbname=postgresql_config["dbname"],
            user=postgresql_config["user"],
            password=postgresql_config["password"],
            host=postgresql_config["host"],
            port=postgresql_config["port"]
        )
        connpostgre.set_client_encoding("utf8")
        print("Connexion à PostgreSQL réussie.")

        """Requêtes PostgreSQL + mesures"""
        optimizations = [
            ("default", utils.postgresql_reset_parameters),
            ("hash join", utils.postgresql_force_hash_join),
            ("nested loop", utils.postgresql_force_nested_loop)
        ]

        for opt_name, opt_function in optimizations:
            opt_function(connpostgre)
            for query in queries:
                elapsed_time = utils.measure_metrics(utils.execute_postgresql_query, connpostgre, query)
                print(f"PostgreSQL - Requête :\n {query}")
                print(f"Temps d'exécution : {elapsed_time:.4f} s")

                result = (
                    f"BDD : PostgreSQL\n"
                    f"Requête : {query.splitlines()[0]}\n"
                    f"Type d'optimisation : {opt_name}\n"
                    f"Temps d'exécution : {elapsed_time:.4f} s\n"
                    f"------------\n"
                )
                append_results_to_file(results_file, result)

        connpostgre.close()
        print("Connexion à PostgreSQL fermée")

    except Exception as e:
        print(f"Erreur lors de la connexion à PostgreSQL : {e}")

if __name__ == "__main__":
    main()