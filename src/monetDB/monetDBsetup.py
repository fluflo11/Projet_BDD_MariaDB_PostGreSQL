import pymonetdb
import utilsMonetDB

def main():
    config_file = "config.yaml"
    db_config = utilsMonetDB.load_config(config_file)
    
    if not db_config:
        print("Erreur : Impossible de charger la configuration")
        return
    
    try:
        conn = pymonetdb.connect(
            hostname=db_config["host"],
            port=db_config["port"],
            username=db_config["user"],
            password=db_config["password"],
            database=db_config["dbname"]
        )
        print("Connexion à MonetDB réussie.")
        utilsMonetDB.drop_all_tables(conn)
        utilsMonetDB.create_tables(conn)
        utilsMonetDB.load_data(conn,"csv")  
        utilsMonetDB.print_tables(conn)
        conn.close()
        print("Connexion à MonetDB fermée.")
        
    except Exception as e:
        print(f"Erreur lors de la connexion à MonetDB : {e}")

if __name__ == "__main__":
    main()