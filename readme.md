# Setup
- 1 : Créer une base de données PostGreSQL et une base de données MonetDB
- 2 : Récupérer une clé d'API Kaggle et ajouter le json fourni par le site au chemin suivant :
  """C:\Users\<username>\. kaggle\kaggle.json."""
- 3 : Installer les librairies manquantes via
    """pip install -r requirements.txt"""
- 4 : Créer manuellement un fichier *results.txt* dans *src/benchmarks/*.

# Utilisation
Le fichier principal à exécuter est *src/main.py*. Une fois l'étape de setup décrite au dessus achevée,
le main s'occupera de run automatiquement les benchmarks. 
Le script *src/benchmarks/benchmarks.py* peut être exécuté directement également afin de lancer un nouveau benchmark.

# Modularité des requêtes
On peut ajouter ou supprimer des requêtes via le fichier de config *config.yaml*.

# Résultats
Les résultats sont stockés dans *src/benchmarks/results.txt*. Le fichier est en gitignore.
