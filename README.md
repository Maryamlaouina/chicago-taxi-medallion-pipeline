# Pipeline Chicago Taxi Trips : datalake médaillon local

Pipeline de données de bout en bout qui **ingère** les trajets de taxi de Chicago depuis l'API publique SODA, les **nettoie** et les **agrège** en tables prêtes pour la BI, selon une architecture médaillon (**Bronze → Silver → Gold**) stockée sur un stockage objet S3-compatible.

- **Orchestration** : Apache Airflow 3
- **Processing** : PySpark 3.5
- **Stockage objet** : MinIO (compatible S3)
- **Exécution locale** : Docker Compose / Podman Compose

---

## 1. Architecture

```
                    API SODA (data.cityofchicago.org)
                                 │  requests + pagination (50 000 lignes/page)
                                 ▼
┌──────────────────────────── MinIO : bucket « datalake » ────────────────────────────┐
│                                                                                     │
│  BRONZE   bronze/taxi_trips/year=YYYY/month=MM/part-*.csv                           │
│           CSV bruts, tels que renvoyés par l'API, partitionnés par mois             │
│                                 │                                                   │
│                                 │  Spark : typage, déduplication, règles qualité    │
│                                 ▼                                                   │
│  SILVER   silver/taxi_trips/year=YYYY/month=MM/*.parquet      (données valides)     │
│           silver/taxi_trips_rejected/*.parquet                (quarantaine + raison)│
│                                 │                                                   │
│                                 │  Spark : règles métier, agrégations, jointure     │
│                                 ▼                                                   │
│  GOLD     gold/daily_kpis/  gold/hourly_patterns/  gold/pickup_zones/               │
│           gold/dim_community_areas/                                                 │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
               Exposition : tables Parquet lisibles en SQL
```

**Orchestration** : un DAG Airflow unique, `chicago_taxi_pipeline` :

```
ingest_bronze [x3, un par mois] → log_bronze_total → transform_silver → build_gold
```

| Service Docker | Rôle |
|---|---|
| `minio` | Stockage objet, socle du datalake |
| `postgres` | Base de métadonnées d'Airflow |
| `airflow-init` | Migration de la base et création de l'utilisateur|
| `airflow-apiserver` | Interface web et API d'Airflow |
| `airflow-scheduler` | Planifie et **exécute** les tâches (LocalExecutor) : c'est ici que tourne Spark |
| `airflow-dag-processor` | Lit les fichiers du dossier `dags/` |

---

## 2. Prérequis

- **Docker Desktop** (Docker Compose v2 inclus) **ou Podman** avec `podman-compose`
- **6 Go de RAM minimum** alloués à Docker/Podman (8 Go conseillés) : Spark s'exécute dans le conteneur `airflow-scheduler`
- **Ports libres** : `8080` (Airflow), `9000` et `9001` (MinIO)
- Une connexion internet (images, dépendances, données de l'API)


> Le projet a été développé et testé avec **Podman** sous Windows. Les commandes Docker équivalentes sont indiquées.

---

## 3. Lancement pas à pas

**1. (Optionnel) Configuration**

Les valeurs par défaut fonctionnent sans rien configurer. Pour les changer (mot de passe MinIO, token SODA) :

```bash
cp .env.example .env    # puis éditer .env
```

**2. Construire l'image et démarrer les services**

```bash
# Docker
docker compose up -d --build

# Podman : construire l'image une seule fois, puis démarrer
podman build -t mini-project-airflow:latest .
podman compose up -d
```

Le premier build prend plusieurs minutes (Java, PySpark, connecteur S3A).

**3. Ouvrir les interfaces**

| Interface | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| MinIO | http://localhost:9001 | `minioadmin` / `minioadmin` |

**4. Lancer le pipeline**

Dans Airflow, ouvrir le DAG `chicago_taxi_pipeline` et cliquer sur **Trigger**. Durée totale : environ **10 à 15 minutes**.

**5. Vérifier le résultat**

Dans MinIO, le bucket `datalake` contient les trois couches `bronze/`, `silver/` et `gold/`.

**Arrêter le projet** : `docker compose down` (ou `podman compose down`). Les données sont conservées dans les volumes ; `down -v` les supprime.

> **Podman** : après une modification du Dockerfile, faire `podman compose down` puis `up -d`. Contrairement à Docker Compose, `podman-compose up` ne recrée pas les conteneurs existants quand l'image change.

---

## 4. Les données

| | |
|---|---|
| **Source** | [Chicago Taxi Trips (2013-2023)](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew), dataset `wrvz-psew` |
| **Méthode** | API SODA, filtre sur `trip_start_timestamp`, pagination par `$limit` / `$offset` triée sur `:id` |
| **Période** | 1er trimestre 2023 (1er janvier → 31 mars 2023) |
| **Volume** | **1 446 439 trajets** (janvier : 414 478) |

La période est définie dans `dags/chicago_taxi_pipeline.py` (liste `MONTHS`) ; le téléchargement est fait par `scripts/ingest_bronze.py`. Pour ingérer une autre période, il suffit de modifier `MONTHS`.

Le tri `$order=:id` est indispensable : sans ordre stable, la pagination pourrait renvoyer des doublons ou sauter des lignes d'une page à l'autre.

---

## 5. Détail des couches

### Bronze : données brutes

- Un mois = une tâche Airflow (*dynamic task mapping*), exécutées en parallèle.
- Chaque page de 50 000 lignes est écrite telle quelle en CSV, directement de l'API vers MinIO.
- Organisation `year=YYYY/month=MM/` : Spark reconnaît ces dossiers comme des colonnes de partition.
- **Idempotence** : le dossier du mois est vidé avant ingestion. Relancer le pipeline donne toujours le même résultat, sans doublons.
- **Robustesse** : 5 réessais avec attente exponentielle par appel API, plus 2 réessais Airflow par tâche.

### Silver : données nettoyées

Les règles ont été définies à partir d'une **exploration des données** (scripts dans `scripts/exploration/`), et chaque seuil est justifié par un chiffre.

**Ce que l'exploration a révélé :**
- Les horodatages sont **arrondis au quart d'heure** par la ville (vie privée) : la durée est donc prise dans `trip_seconds`, pas calculée par différence.
- Aucun doublon sur `trip_id` (la déduplication est gardée par sécurité).
- Des valeurs aberrantes : trajets de 24 h, 899 miles, montants de 9 999,75 $.
- 10,7 % des trajets ont une distance de 0 mile, mais la plupart ont une vraie durée (médiane 4,5 min) et un vrai prix (médiane 7,25 $) : distance non mesurée, pas faux trajet.

**Règles appliquées :**

| Règle | Seuil | Justification                                                      |
|---|---|--------------------------------------------------------------------|
| Déduplication | sur `trip_id` | sécurité en cas de double ingestion                                |
| Champs obligatoires | dates, durée, distance, montants non nuls | indispensables aux KPIs                                            |
| Cohérence temporelle | fin ≥ début | une course ne peut pas finir avant d'avoir commencé                                        |
| Durée | 60 s ≤ durée ≤ 4 h | < 60 s : courses annulées ; 99 % des trajets durent < 65 min       |
| Distance | ≤ 100 miles | 99 % des trajets font < 25,5 miles                                 |
| Prix | `fare` ≥ 3,25 $ | tarif de prise en charge à Chicago (et 1er percentile des données) |
| Total | `trip_total` ≤ 1 000 $ | élimine les erreurs de saisie (9 999,75 $ ...)                     |
| Distance à 0 | remplacée par `NULL` | garde le trajet (revenu réel) sans fausser la distance moyenne     |
| Colonnes supprimées | `*_census_tract`, `*_centroid_location` | 62 % de nulls / redondantes avec lat-lon                           |

**Quarantaine** : les lignes rejetées ne sont pas supprimées silencieusement. Elles sont écrites dans `silver/taxi_trips_rejected/` avec une colonne `reject_reason`.

**Rapport qualité** (affiché dans les logs de la tâche de Airflow) :

| Raison du rejet | Lignes |
|---|---:|
| `invalid_duration` | 70 570 |
| `missing_amount` | 1 251 |
| `invalid_amount` | 856 |
| `missing_duration` | 262 |
| `invalid_distance` | 123 |
| `missing_timestamp` | 27 |
| `missing_distance` | 5 |
| **Total rejeté** | **73 094 (5,05 %)** |

```
Bronze 1 446 439 = doublons 0 + rejetées 73 094 + Silver 1 373 345   ✔
```

Ce contrôle est vérifié par une assertion à chaque exécution : **aucune ligne ne disparaît sans explication**.

### Gold : tables analytiques

Les règles **métier** sont appliquées ici, pas en Silver (Silver = qualité technique, Gold = définition des indicateurs) :

- **Revenu** = somme de `trip_total`, **hors paiements `No Charge` et `Dispute`** (revenu réellement encaissé). Ces trajets restent comptés dans le nombre de trajets et la durée moyenne.
- **Revenu moyen par trajet** = revenu / nombre de trajets payés.
- **Durée moyenne** en minutes, basée sur `trip_seconds`.
- **Distance moyenne** calculée sur les trajets dont la distance est connue.
- Montants en `decimal` (et non `double`) pour éviter les erreurs d'arrondi sur de l'argent.

| Table | Grain | Lignes | Contenu |
|---|---|---:|---|
| `daily_kpis` | 1 ligne / jour | 90 | trajets, revenu, revenu moyen, durée et distance moyennes, pourboires |
| `hourly_patterns` | 1 ligne / heure (0-23) | 24 | mêmes indicateurs + trajets moyens par jour : heures de pointe |
| `pickup_zones` | 1 ligne / quartier | 77 | indicateurs par quartier de prise en charge, nom du quartier, **rang** |
| `dim_community_areas` | 1 ligne / quartier | 77 | référentiel numéro → nom des 77 quartiers officiels |

`pickup_zones` est obtenue par une **jointure** entre les agrégats par quartier et le référentiel des quartiers (on agrège d'abord, 1,37 M → 77 lignes, puis on joint).

**Aperçu des résultats** (top 5 des zones de prise en charge) :

| Rang | Quartier | Trajets | Revenu ($) | Revenu moyen ($) | Durée moy. (min) |
|---:|---|---:|---:|---:|---:|
| 1 | Near North Side | 291 979 | 4 451 076 | 15,25 | 12,7 |
| 2 | O'Hare (aéroport) | 234 279 | 12 374 252 | 52,92 | 30,9 |
| 3 | Loop | 220 906 | 3 418 397 | 15,48 | 12,6 |
| 4 | Near West Side | 134 417 | 2 016 497 | 15,01 | 13,3 |
| 5 | Lake View | 47 939 | 829 390 | 17,31 | 16,0 |

O'Hare est 2e en nombre de trajets mais 1er en revenu, de loin. Garfield Ridge (7e, quartier de l'aéroport Midway) présente le même profil : les deux aéroports concentrent les courses les plus longues et les plus rémunératrices.
Les resultas sont aussi affichés sur les logs des taches Airflow 

---

## 6. Choix techniques et arbitrages

| Choix | Pourquoi | Limite assumée |
|---|---|---|
| **Spark en mode local** dans le conteneur Airflow | Simple, fiable, suffisant pour ~1,4 M lignes | Pas de distribution réelle ; en production : cluster Spark séparé |
| **Image Docker personnalisée** (Java + PySpark + jars S3A) | Dépendances installées au build : démarrage rapide et reproductible | Build initial long (~300 Mo de PySpark) |
| **Un seul DAG** | Lisible, simple à déboguer, un clic lance tout | Relancer une couche seule passe par *Clear* dans l'interface |
| **Dynamic task mapping** par mois | Parallélisme, et un mois en échec se relance seul | — |
| **CSV en Bronze, Parquet en Silver/Gold** | Bronze = copie fidèle de la source ; Parquet = typé, compressé, rapide | — |
| **Recalcul complet** de Silver et Gold (`overwrite`) | Idempotent et simple à ce volume | Ne passerait pas à l'échelle ; en production : traitement incrémental |
| **`shuffle.partitions = 8`** et `repartition` avant écriture | Évite le *small files problem* (600+ fichiers de 1 Mo → 1 fichier par mois) | Valeur adaptée au local, à revoir sur un cluster |
| **Quarantaine** des rejets | Traçabilité : on sait ce qui a été écarté et pourquoi | — |
| **Référentiel des quartiers** dans le code | 77 lignes fixes, rend Gold lisible et permet la jointure | Donnée statique|
| **Scripts métier séparés des DAGs** (`scripts/`) | Testables hors Airflow, DAG réduit à l'orchestration | — |

---

## 7. Exposition des données

La couche **Gold** est le point d'entrée unique des équipes BI : des tables petites, agrégées, aux noms explicites, avec des définitions documentées ci-dessus. Les analystes n'accèdent jamais à Bronze ni à Silver, ce qui garantit que tout le monde utilise les mêmes chiffres.

Les tables sont stockées en **Parquet**, un format standard lisible par la plupart des outils :
- **en production**, on les exposerait via un moteur SQL branché sur le stockage objet (Trino, Athena, Databricks SQL…), auquel se connectent les outils de BI (Power BI, Tableau, Metabase, Superset) ;
- **en local**, elles sont directement interrogeables en SQL avec un moteur léger comme DuckDB.

---

## 8. Ce que j'aurais fait avec plus de temps

- **Format de table Apache Iceberg** pour Silver et Gold, avec un catalogue REST : transactions ACID (pas de lecture d'une table à moitié écrite), évolution de schéma, *time travel*, et un schéma partagé entre Spark et le moteur SQL.
- **Couche d'exposition SQL** avec **Trino** branché sur ce catalogue, et un tableau de bord (Superset ou PowerBI par exemple).
- **Découpage en trois DAGs** (Bronze, Silver, Gold) reliés par des **Assets** Airflow 3 : chaque couche relançable indépendamment, avec son propre planning.
- **Ingestion incrémentale quotidienne** basée sur la date logique d'Airflow, avec `catchup` pour rattraper automatiquement les jours manqués.
- **Tests automatisés** : tests unitaires des règles de nettoyage sur de petits DataFrames, et tests de qualité des données (Great Expectations ou Soda).
- **Cluster Spark séparé** (ou `SparkSubmitOperator`) pour découpler l'orchestration du calcul.
- **Gestion des secrets** : identifiants via un gestionnaire de secrets ou les connexions Airflow, plutôt que des variables d'environnement par défaut.
- **Supervision** : alertes en cas d'échec ou de taux de rejet anormal via des protocole comme SMTP que j'ai déjà utilsé dans certain projet.

---

## 9. Structure du projet

```
.
├── dags/
│   └── chicago_taxi_pipeline.py     # DAG : Bronze → Silver → Gold
├── scripts/
    ├── config.py                    # Paramètres centraux : source, période, chemins du datalake
│   ├── ingest_bronze.py             # API SODA → bronze/ (pagination, réessais, idempotence)
│   ├── transform_silver.py          # Bronze → Silver : typage, qualité, quarantaine
│   ├── build_gold.py                # Silver → Gold : règles métier, agrégations, jointure
│   ├── community_areas.py           # Référentiel des 77 quartiers de Chicago
│   ├── spark_utils.py               # Session Spark configurée pour MinIO
│   ├── minio_utils.py               # Client S3, création de bucket, suppression de dossier
│   └── exploration/                 # Analyses ponctuelles ayant fixé les règles Silver
│       ├── explore_bronze.py
│       ├── explore_numbers.py
│       └── explore_thresholds.py
├── Dockerfile                       # Image Airflow + Java 17 + PySpark + connecteur S3A
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```
## 10. Lancement

**Prérequis** : Docker ou Podman, 6 Go de RAM minimum alloués.

```bash
# Docker
docker compose up -d 

# Docker with build (if dockerfile has been changed)
docker compose up -d --build

# Podman
podman compose up -d

# Podman with build (if dockerfile has been changed)
podman compose up -d --build 

```

Puis ouvrir Airflow sur http://localhost:8080 (`airflow` / `airflow`) et lancer le DAG `chicago_taxi_pipeline` avec **Trigger**.

Les résultats sont visibles dans MinIO sur http://localhost:9001 (`minioadmin` / `minioadmin`), bucket `datalake`.

**Arrêter** : `docker compose down` (ou `podman compose down`).


Les scripts peuvent aussi être lancés hors Airflow, par exemple :

```bash
docker compose exec -w /opt/project airflow-scheduler python -m scripts.transform_silver
```
ou

```bash
podman exec -w /opt/project mini_project_airflow-scheduler_1 python -m scripts.transform_silver
```