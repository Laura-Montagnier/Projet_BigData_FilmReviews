import json
import os

def afficher_premieres_lignes_json(fichier, nb_lignes=5):
    try:
        # Obtenir la taille du fichier en Go
        taille_fichier = os.path.getsize(fichier) / (1024 ** 3)  # Convertir en Go

        # Afficher le nom, la taille et le nombre de lignes du fichier
        print(f"\nAffichage des premières lignes et informations de : {fichier}")
        print(f"Taille du fichier : {taille_fichier:.4f} Go")

        # Compter le nombre de lignes dans le fichier
        with open(fichier, 'r', encoding='utf-8') as f:
            ligne_count = sum(1 for line in f)
        print(f"Nombre de lignes dans le fichier : {ligne_count}")

        print("=" * 60)

        with open(fichier, 'r', encoding='utf-8') as f:
            # Lire les premières lignes sans charger tout le fichier
            lines = []
            for i in range(nb_lignes):
                line = f.readline()
                if line:
                    lines.append(line.strip())  # Supprimer les espaces inutiles
                else:
                    break

            # Afficher les premières lignes
            print("Premières lignes du fichier :")
            for line in lines:
                print(line)
            
            # Revenir au début du fichier pour charger les données partiellement
            f.seek(0)
            premier_element = json.load(f)  

            print("\nQuelques informations sur le fichier JSON :")
            if isinstance(premier_element, dict):
                print(f"- Type de données : Dictionnaire")
                print(f"- Clés disponibles : {list(premier_element.keys())[:5]}...")  
            elif isinstance(premier_element, list):
                print(f"- Type de données : Liste")
                print(f"- Nombre d'éléments dans la liste : {len(premier_element)}")
                print(f"- Quelques éléments (premiers 3) : {premier_element[:3]}")  
            else:
                print(f"- Type de données inconnu au début : {type(premier_element)}")

        print("=" * 60)

    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {fichier} : {e}")
        print("=" * 60)


# Chemin vers le répertoire contenant les fichiers JSON
repertoire_raw = "raw"

# Vérifier si le répertoire existe avant d'itérer sur les fichiers
if os.path.exists(repertoire_raw) and os.path.isdir(repertoire_raw):
    for nom_fichier in os.listdir(repertoire_raw):
        if nom_fichier.endswith('.json'):
            if nom_fichier == "reviews.json":
                continue
            chemin_fichier = os.path.join(repertoire_raw, nom_fichier)
            afficher_premieres_lignes_json(chemin_fichier)
else:
    print(f"Erreur : Le répertoire '{repertoire_raw}' n'existe pas.")
