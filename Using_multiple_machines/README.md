# Partie 1 : Execution avec Spark Standalone et hadoop sur une seule machine

## 1. Lancer le master et le worker spark

**Objectif :** Avoir un cluster pour executer du calcul distribué en mémoire.

**On se place dans :** le dossier où on à téléchargé spark pour éxecuter les commandes suivantes.
EX : Pour moi "~/Documents/ENSTA_3A/Big_Data/Lab_3_fichier_prof/mon_env/bin".

Master :

```bash
./spark-class org.apache.spark.deploy.master.Master
```

On récupère ensuite l'addresse de notre master en suivant le lien : **http://localhost:8080**

Worker :

On remplace l'addresse par celle de notre master.

```bash
./spark-class org.apache.spark.deploy.worker.Worker spark://147.250.234.244:7077
```
Ou bien on execute directement, pour que l'addresse soit recupérée elle meme :

```bash
export SPARK_MASTER_URL=$(curl -s http://localhost:8080 | grep -oP '(?<=spark://)[^:]+:\d+' | head -n1)
./spark-class org.apache.spark.deploy.worker.Worker spark://$SPARK_MASTER_URL
```

## 2. Lancer un NameNode et un DataNode

**Objectif :** Avoir un système de fichiers distribué (HDFS) pour stocker et partager les données entre les nœuds du cluster.

Préparation :
```bash
"$HADOOP_HOME/bin/hdfs" namenode -format
```

NameNode :
```bash
"$HADOOP_HOME/bin/hdfs" namenode
```
DataNode :
```bash
"$HADOOP_HOME/bin/hdfs" datanode
```

On peut avoir les informations sur le cluster à l'addresse : **http://localhost:9870**

## 3. Ajouter le fichier JSON au système HDFS

Préparation, il faut se créer un dossier utilisateur dans notre cluster :

```bash
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER"
```
Puis on ajoute notre json :

```bash
"$HADOOP_HOME/bin/hdfs" dfs -put raw/ratings.json
"$HADOOP_HOME/bin/hdfs" dfs -ls
```

## 4. Executer la tâche spark 

**On se place dans :** le dossier ou se trouve le code pour éxecuter les commandes suivantes.


```bash
spark-submit --master spark://129.104.73.59:7077 code.py
```
ou 
```bash
export SPARK_MASTER=$(curl -s http://localhost:8080 | grep -oP '(?<=spark://)[^:]+:\d+' | head -n 1)
spark-submit --master spark://$SPARK_MASTER code.py
```


# Partie 2 : Exécution avec Spark Standalone et Hadoop sur deux machines

## **Introduction**
Lorsque les volumes de données deviennent trop grands pour être stockés et traités sur une seule machine, il est nécessaire d'adopter une approche distribuée. C'est pourquoi nous utilisons :  

1. **Un cluster Spark** pour paralléliser le calcul et exécuter les tâches sur plusieurs machines. Cela permet d'améliorer la rapidité et l'efficacité des traitements analytiques en répartissant la charge de travail.  
2. **Un système de fichiers distribué (HDFS)** pour stocker les données de manière répliquée et distribuée sur plusieurs machines. HDFS permet ainsi de garantir la tolérance aux pannes et d'accéder aux données de manière efficace depuis n'importe quel nœud du cluster Spark.

Dans cette partie, nous allons configurer un cluster Spark et un cluster HDFS sur **deux machines**, afin de simuler un environnement de calcul distribué.

---

## **1. Configuration du cluster Spark sur deux machines**
Nous utilisons deux machines :
- **Master (M1) :** Blandine
- **Worker (M2) :** Laura

### **Sur la machine Master (M1)**
1. Démarrer le Master Spark  
   ```bash
   ./spark-class org.apache.spark.deploy.master.Master  
   ```
2. Vérifier que le Master est actif  
   Accéder à **http://129.104.73.59:8080** pour voir l’interface du cluster Spark.

### **Sur la machine Worker (M2)**
1. Démarrer un Worker et le connecter au Master 
   Remplacer par l'addresse du master :
   ```bash 
   ./spark-class org.apache.spark.deploy.worker.Worker spark://129.104.73.59:7077  
   ```
2. Vérifier que le Worker est bien connecté  

---

## **2. Configuration de HDFS sur deux machines**

Amélioration envisagée.

---

## **3. Exécuter la tâche Spark en utilisant HDFS**
Une fois Spark et HDFS configurés, nous pouvons exécuter notre tâche Spark en lisant directement les données depuis HDFS.  

Sur **n'importe quelle machine du cluster**, exécuter comme dans l'éape précédente.

**Spark distribuera les calculs entre les machines M1 et M2 en lisant les données depuis HDFS.**

---

## **Conclusion**
Avec cette architecture :
- **Le Master Spark (M1) orchestre l'exécution des tâches.**
- **Le Worker Spark (M2) exécute les calculs Spark.**
- **Le NameNode HDFS (M1) gère les métadonnées et la gestion des fichiers.**
- **Le DataNode HDFS (M2) stocke les fichiers de manière distribuée.**

Nous avons ainsi un **environnement distribué fonctionnel avec Spark et HDFS**, permettant d'effectuer des traitements massivement parallèles sur de grands volumes de données. 

# TO DO : 

- Ajouter des print et des tests pour vérif chaque étape
  - Visualiser les tâches spark --> pourquoi il y a des stages 
  - Est ce que le fichier est présent en copies multiples sur mon systeme HDFS ?
- Mettre les valeurs qui dépendant des configs en string au début du fichier 