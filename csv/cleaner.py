import pandas as pd
import re
import os

"""
Permet de nettoyer un fichier csv. En effet, certains présentent une anomalie assez problématique
(entiers suivis d'une virgule devant les lignes)    
"""
def clean_classic(pathinput,pathoutput):
    df = pd.read_csv(pathinput)
    df = df.applymap(lambda x: re.sub(r'^\d+?,', '', str(x)) if isinstance(x, str) else x) #tf???
    df.drop(df.columns[0], axis=1, inplace=True) #retire les colonnes en trop
    df.to_csv(pathoutput, index=False)
            
def main():
    input_directory = 'csv'
    output_directory = 'csv'

    files = ['yelp_academic_dataset_review.csv',
             'yelp_academic_dataset_business.csv',
             'yelp_academic_dataset_tip',
             'yelp_academic_dataset_user.csv']
    
    for file in files:
        print("Debut nettoyage csvs : " + file)
        input_file = os.path.join(input_directory, file)
        output_file = os.path.join(output_directory, file)
        clean_classic(input_file, output_file)

if __name__ == "__main__":
    main()