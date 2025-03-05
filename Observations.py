import json
import os

def afficher_premieres_lignes_json(fichier, nb_lignes=5):
    try:
        # Afficher le nom du fichier pour plus de clarté
        print(f"\nAffichage des premières lignes et informations de : {fichier}")
        print("=" * 60)

        with open(fichier, 'r', encoding='utf-8') as f:
            # Essayer de lire les premières lignes sans tout charger en mémoire
            lines = []
            for i in range(nb_lignes):
                line = f.readline()
                if line:
                    lines.append(line)
                else:
                    break

            # Afficher les premières lignes
            print("Premières lignes du fichier :")
            for line in lines:
                print(line.strip())
            
            # Essayer de charger le fichier pour extraire quelques informations
            f.seek(0)  # Revenir au début du fichier
            premier_element = json.load(f)  # Lire un seul élément du JSON
            print("\nQuelques informations sur le fichier JSON :")
            
            # Vérifier le type de données pour afficher des informations pertinentes
            if isinstance(premier_element, dict):
                print(f"- Type de données : Dictionnaire")
                print(f"- Clés disponibles : {list(premier_element.keys())[:5]}...")  # Limiter à 5 clés
            elif isinstance(premier_element, list):
                print(f"- Type de données : Liste")
                print(f"- Nombre d'éléments dans la liste : {len(premier_element)}")
                print(f"- Quelques éléments (premiers 3) : {premier_element[:3]}")  # Afficher les 3 premiers éléments
            else:
                print(f"- Type de données inconnu au début : {type(premier_element)}")

            print("=" * 60)

    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {fichier} : {e}")
        print("=" * 60)


# Chemin vers le répertoire contenant les fichiers JSON
repertoire_raw = "genome_2021/movie_dataset_public_final/raw"

# Parcourir tous les fichiers dans le répertoire 'raw'
for nom_fichier in os.listdir(repertoire_raw):
    # Vérifier que le fichier est bien un fichier JSON
    if nom_fichier.endswith('.json'):
        chemin_fichier = os.path.join(repertoire_raw, nom_fichier)
        print(f"\nPremières lignes du fichier : {chemin_fichier}")
        afficher_premieres_lignes_json(chemin_fichier)
