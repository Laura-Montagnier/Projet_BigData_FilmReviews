# Execution avec Spark Standalone sur une seule machine


## 1. Lancer le master et le worker spark

**Objectif :** Avoir un cluster pour executer du calcul distribué en mémoire.

Depuis "~/Documents/ENSTA_3A/Big_Data/Lab_3_fichier_prof/mon_env/bin".

Master :

```bash
./spark-class org.apache.spark.deploy.master.Master
```

Worker :

On remplace l'addresse par celle de notre master.

```bash
./spark-class org.apache.spark.deploy.worker.Worker spark://129.104.73.59:7077
```

On peut avoir les informations sur le cluster à l'addresse : **http://localhost:8080**

## 2. Lancer un NameNode et un DataNode

**Objectif :** Avoir un cluster pour distribuer notre donnée.

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

## 3. Ajouter le json au cluster HDFS

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

```bash
spark-submit --master spark://129.104.73.59:7077 code.py
```


# TO DO : 

- Ajouter des print et des tests pour vérif chaque étape
- Mettre les valeurs qui dépendnat des configs en string au début du fichier 