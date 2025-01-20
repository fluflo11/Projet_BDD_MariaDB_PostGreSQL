import psycopg2
import utilsPostGreSQL

def main():

    config_file = "config.yaml"
    db_config = utilsPostGreSQL.load_config(config_file)

    try:
        conn = psycopg2.connect(
            options="-c client_encoding=utf-8",
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        conn.set_client_encoding("utf8")
        print("Connexion à PostgreSQL réussie.")
        utilsPostGreSQL.create_tables(conn)
        utilsPostGreSQL.load_data(conn,"csv")
        conn.close()
        print("Connexion à PostGreSQL fermée")
    except Exception as e:
        print(f"Erreur lors de la connexion à PostgreSQL : {e}")

if __name__ == "__main__":
    main()