import os
import yaml
import pandas

def load_config(config_file):
    try:
        with open(config_file, "r",encoding="utf-8") as file:
            config = yaml.safe_load(file)
        return config["postgresql"]
    except Exception as e:
        print(f"Erreur load_config : {e}")
        exit(1)
        
"""
Permet de créer les tables nécessaires au COPY INTO
"""
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
            date TEXT
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
        print("Tables créées dans PostGreSQL.")
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
                print(f"Chargement de {table}")
                file_path = os.path.abspath(os.path.join(csv_folder, file_name))
                table_name = f'"{table}"' if "_" in table or table.isupper() else table
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        cursor.copy_expert(
                            sql=f"""
                            COPY {table_name} FROM STDIN 
                            WITH (
                                FORMAT CSV,
                                DELIMITER ',',
                                QUOTE '"',
                                ENCODING 'UTF-8'
                            )
                            """,
                            file=f
                        )
                    conn.commit()
                    print(f"Table : {table} remplie.")
                except Exception as e:
                    print(f"Erreur dans {table} : {e}")
                    conn.rollback() #indispensable
            print("Importation PostGreSQL finie.")
            
    except Exception as e:
        print(f"Exception : {e}")
