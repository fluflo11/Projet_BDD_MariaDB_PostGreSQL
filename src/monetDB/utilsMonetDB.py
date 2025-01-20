import yaml
import os

def load_config(config_file):
    try:
        with open(config_file, encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config.get('monetdb')
    except Exception as e:
        print(f"Erreur load config : {e}")
        return None

def create_tables(conn):
    commands = [
        """
        CREATE TABLE IF NOT EXISTS BUSINESS (
            business_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            postal_code VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR,
            stars VARCHAR,
            review_count VARCHAR,
            is_open VARCHAR,
            attributes VARCHAR,
            categories VARCHAR,
            hours VARCHAR
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS CHECKIN (
            business_id VARCHAR REFERENCES BUSINESS(business_id),
            date STRING
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS TIP (
            user_id VARCHAR,
            business_id VARCHAR REFERENCES BUSINESS(business_id),
            text VARCHAR,
            date VARCHAR,
            compliment_count VARCHAR
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS APP_USER (
            user_id VARCHAR PRIMARY KEY,
            name VARCHAR ,
            review_count VARCHAR,
            yelping_since VARCHAR,
            useful VARCHAR,
            funny VARCHAR,
            cool VARCHAR,
            elite VARCHAR,
            friends VARCHAR,
            fans VARCHAR,
            average_stars VARCHAR,
            compliment_hot VARCHAR,
            compliment_more VARCHAR,
            compliment_profile VARCHAR,
            compliment_cute VARCHAR,
            compliment_list VARCHAR,
            compliment_note VARCHAR,
            compliment_plain VARCHAR,
            compliment_cool VARCHAR,
            compliment_funny VARCHAR,
            compliment_writer VARCHAR,
            compliment_photos VARCHAR
        )
        """
    ]

    try:
        with conn.cursor() as cursor:
            for command in commands:
                cursor.execute(command)
            conn.commit()
        print("Tables MonetDB crées.")
    except Exception as e:
        print(f"Exception : {e}")
        
def load_data(conn, csv_folder):
    try:
        with conn.cursor() as cursor:
            tables = {
                "business": "yelp_academic_dataset_business.csv",
                "checkin": "yelp_academic_dataset_checkin.csv",
                "tip": "yelp_academic_dataset_tip.csv",
                "app_user": "yelp_academic_dataset_user.csv"
            }
            for table, file_name in tables.items():
                if table == "tip":
                    cursor.execute("ALTER TABLE TIP DROP CONSTRAINT tip_business_id_fkey;") #Mysterious hack
            for table, file_name in tables.items():
                file_path = os.path.abspath(os.path.join(csv_folder, file_name))
                file_path = file_path.replace('\\', '/')
                print(f"Chargement de {file_path} dans la table {table}...")
                table_name = f'"{table}"' if "_" in table or table.isupper() else table
                query = f"""
                    COPY INTO {table_name}
                    FROM '{file_path}'
                    USING DELIMITERS ',', E'\\n', '"'
                    BEST EFFORT;
                """ 
                try:
                    cursor.execute(query)
                    conn.commit()
                    print(f"Importation réussie pour la table {table}.")
                except Exception as e:
                    print(f"Erreur pour la table {table} : {e}")
                    conn.rollback()
                    
            print("Données importées dans MonetDB.")
            cursor.execute("ALTER TABLE TIP ADD CONSTRAINT tip_business_id_fkey FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id);") #On doit recup une exception là dessus mais pg la table est déjà remplie
            conn.commit()
    except Exception as e:
        print(f"Exception : {e}")
        
"""
Permet de supprimer les tables en cas de soucis.
"""
def drop_all_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM tables
            WHERE system=false;
        """)
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            print(f"Table: {table_name} supprimée")
            cursor.execute(f"DROP TABLE {table_name} CASCADE;")
        conn.commit()
        print("Tables supprimées")
    except Exception as e:
        print(f"Exception : {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def print_tables(conn):
    try:
        with conn.cursor() as cursor:
            tables = ["business", "checkin", "tip", "app_user"]
            for table in tables:
                query = f"SELECT COUNT(*) FROM {table};"
                cursor.execute(query)
                count = cursor.fetchone()[0]
                print(f"Table: {table}, Nombre de lignes: {count}")
    except Exception as e:
        print(f"Exception : {e}")


