import kagglehub
import shutil
import os
import subprocess

"""Télécharger les fichiers csv utilisés pour le benchmark"""

path = kagglehub.dataset_download("flyersteve/yelp-csv")
print("Path to dataset files:", path)

destination_path = os.path.join(os.getcwd(), "csv")
if not os.path.exists(destination_path):
    os.makedirs(destination_path)
    
for file_name in os.listdir(path):
    source_file = os.path.join(path, file_name)
    destination_file = os.path.join(destination_path, file_name)
    if os.path.isfile(source_file):
        shutil.copy2(source_file, destination_file)
        print(f"Copié : {source_file} -> {destination_file}")

print("Les fichiers ont été copiés dans csv")


""" Nettoyer les fichiers téléchargés """

print("Début nettoyage fichiers")

cleaner_res = subprocess.run(["python", "csv/cleaner.py"], capture_output=True, text=True)
if cleaner_res.returncode != 0 :
    print("Erreur lors du nettoyage des fichiers : ", cleaner_res)
