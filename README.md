# Creation et alimentation d'un data warehouse en utilisant Airflow 

Ce projet a pour objectif de monter en compétences sur la techno Airflow. Pour ce faire, l'hyptohèse de création d'un dataware house a été retenue. L'objectif est de construire un worflow permettant d'alimenter le data warehouse créée afin d'analyser les données.
Les données sont un jeu de données factice issu d'une activité de vente de produits à l'internationnal.

## Configuration de airflow : 
- LocalExecutor
- PostrgreSql MetaDabase
- PostreSql pour le data warehouse

## Modélisation du data Warehouse
Le data warehouse a été modélisé en utilisant la méthodologie de dénormalisation dite du "Star Schema", ci dessous le résultat de la modélisation

![alt](dw_schema.PNG)


## DAG implémenté

Ci dessous, une vue du web server airflow montrant le DAG implémenté pour alimenter le data warehouse

![alt](airflow.PNG)