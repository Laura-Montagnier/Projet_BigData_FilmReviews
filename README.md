# 1. Observations
# 2. Note moyenne sur chaque film
# 3. Note moyenne triée

Le fichier concerné est Best_rated_movies.py. On utilise tout d'abord ce qu'on avait vu en TP pour charger les JSONs avec leur schéma dans des DataFrames. On a deux JSONs, celui contenant les votes (ratings.json) et celui contenant les métadatas (metadata.json).\
Au début, on triait simplement par note ("rating"), mais on avait plein de films dont la note était de 5/5. Après une courte réflexion, on a compris que certains films avaient été notés par une seule, ou deux ou trois personnes. On a donc eu l'idée de compter le nombre de votes, de trier à la fois par note, puis pour les notes identiques par nombre de votes, et d'accepter uniquement les notes auxquelles plus de 50 personnes ont contribué.\

```
+--------------------------------+------------------+------------+\             
|title---------------------------|avg_rating--------|count_rating|\
+--------------------------------+------------------+------------+\
|Planet Earth II (2016)----------|4.483695652173913-|1104--------|\
|Planet Earth (2006)-------------|4.464009518143962-|1681--------|\
|Shawshank Redemption, The (1994)|4.4231612557721265|98967-------|\
|Band of Brothers (2001)---------|4.389901290812452-|1317--------|\
|Cosmos--------------------------|4.385931558935361-|263---------|\
|Cosmos: A Spacetime Odissey-----|4.358333333333333-|180---------|\
|Twin Peaks (1989)---------------|4.354785478547854-|303---------|\
|Godfather, The (1972)-----------|4.332356046454966-|61565-------|\
|Blue Planet II (2017)-----------|4.318456883509834-|661---------|\
|Usual Suspects, The (1995)------|4.290028526351017-|62749-------|\
+--------------------------------+------------------+------------+\
``` 


# 4. Faire tourner sur deux machines
# 5. Faire tourner sur un cluster hadoop
# 6. Charger directement les fichiers pour pas avoir à download
