# Marvel Heroes

#### Groupe
- Cédric GARCIA - FIL A3
- Benjamin RIVARD - FIL A3
#

L'objectif de cet exerice est de développer une application web permettant de découvrir des Héros de Comics (Marvel et DC Comics).

Les principales fonctionnalités de l'application sont : 
* Recherche de Héros
  * La recherche est une recherche full-text, se basant sur les éléments suivants des Héros, par ordre de priorité :
    * Nom
    * Alias et identité secrète
    * Description
    * Partenaires
* Suggestion de Héros
  * La suggestion doit tenir compte également des alias et identités secrètes des Héros
* Affichage de la fiche détaillée d'un Héros
* Affichage des 5 dernières fiches consultées
* Affichage du top 5 des fiches consultées
* Statistiques :
  * Répartition des Héros par univers (Marvel / DC Comics).
  * Répartition des Héros par année d'apparition et par univers.
  * Top 5 des super-pouvoirs.

Techniquement : 
* La recherche et la suggestion se basent sur Elasticsearch
* La fiche détaillée et les statistiques se basent sur MongoDB
* Les hits (5 dernières fiches + top 5) se basent sur Redis

Votre mission est la suivante : compléter le code manquant afin de faire fonctionner complètement l'application !

Les éléments à compléter se trouvent dans les fichiers suivants : 
* Scripts d'import des données
  * `scripts/import-elasticsearch.js`
  * `scripts/import-mongo.js`
* Application
  * `app/repository/ElasticRepository.java`
  * `app/repository/MongoDBRepository.java`
  * `app/repository/RedisRepository.java`


## Pré-requis

Datastores : 
* Elasticsearch (version 7.5.x)
* MongoDB (version 4.2.x)
* Redis (version 5.0.x)

Languages et tooling :
* Java (version 11)
* SBT (version 1.2.x ou >)
* Node (version 10.x)
* NPM (version 6.x)

Environnement de développement conseillé :
* Intellij Idea (pour la partie Play/Java)
  * + plugin SBT : https://plugins.jetbrains.com/plugin/5007-sbt
* VSCode (pour la partie Node)


## Dataset

Les données sont disponibles dans le dossier `scripts/all-heroes.csv` et proviennent de plusieurs sources de données : 
* [API Marvel](https://developer.marvel.com/)
* [SuperheroDB](https://www.superherodb.com/)


## Liens utiles
* Suggestion avec Elasticsearch : https://www.elastic.co/guide/en/elasticsearch/reference/7.5/search-suggesters.html#completion-suggester
* Aggrégations avec MongoDB : https://docs.mongodb.com/manual/meta/aggregation-quick-reference/


## Pour rendre le travail

Une fois l'ensemble du code complété et que l'appli est fonctionnelle, faites une Pull Request !
