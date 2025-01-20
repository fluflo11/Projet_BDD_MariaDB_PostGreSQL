import sys
import os
import subprocess


"""Setup de l'environnement"""
print("Début setup")
setup_res = subprocess.run(["python", "setup.py"])
if setup_res.returncode != 0 :
    print("Erreur lors du setup : ", setup_res)
else :
    print("Setup de l'environnement réussi")

"""Setup postGreSQL"""
print("Début setup postGreSQL")
setup_postgre_res = subprocess.run(["python", "postGreSQL/postGreSQLsetup.py"])
if setup_postgre_res.returncode != 0 :
    print("Erreur lors du setup : ", setup_postgre_res)
else :
    print("Setup postGreSQL réussi")
    
"""Setup monetDB"""
print("Début setup monetDB")
setup_monetdb_res = subprocess.run(["python", "src\monetDB\monetDBsetup.py"])
if setup_postgre_res.returncode != 0 :
    print("Erreur lors du setup : ", setup_postgre_res)
else :
    print("Setup postGreSQL réussi")
    
"""Benchmarking"""
print("Début benchmarks")
benchmarks_res = subprocess.run(["python", "src\\benchmarks\\benchmarks.py"])
if benchmarks_res.returncode != 0 :
    print("Erreur lors des benchmarks :", benchmarks_res)
else : 
    print("Fin des benchmarks")
    
print("Fin de la session de benchmarking")