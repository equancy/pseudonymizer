# Pré requis
Python 3 doit être installé sur la machine bastion. Sur RHEL:

```
sudo yum install python3
sudo alternatives --set python /usr/bin/python3
```

## Présentation du répertoire

```bash
│   ├── pseudonymizer-cli-runner.py/  	<- Permet d\'executer le module à partir d\'un script
│   ├── pseudonymizer				<- Code source du module \'pseudonymizer\'
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── cli.py					<- Script appelé par la commande \'pseudonymizer-cli\'. Dispatch les arguments grace la librairie @click
│   │   ├── config
│   │   │   └── main.py     		<- Contient les variables de configuration.
│   │   ├── data_catalog.py 		<- Class utilitaire pour construire une version python du data catalog
│   │   ├── dataframe_handler.py	<- Fonctions utilitaires pour gérer les manipulations éventuelles des Dataframes @pandas
│   │   ├── pseudonymize.py			<- Fonction de hashage par HMAC
│   │   ├── hashkey_store.py		<- Fonction de hashage par HMAC
│   │   └── utils/					<- Peut contenir des scripts utilitaire générique
│   │   └── exceptions/				<- Contient les exceptions customisées
│   ├── pseudonymizer_cli.egg-info
│   ├── setup.cfg					<- Fichier de configuration du module
│   └── setup.py					<- Sert à la construction du module avec @setuptools
│   └── .env						<- !! Contient les variables d\'environmments !!
│   └── .env.example				<- Fichier décrivant les variables d\'environnement nécessaire au bon fonctionnement du module
└── test_environment.py
```

## Dépendances Python 
Les dépendances du projet se trouvent dans le fichier 
`~/WORK_DIR/pseudonymizer/src/setup.py`

* [Click](https://click.palletsprojects.com/en/7.x/)==7.0
* [PyYAML](https://pypi.org/project/PyYAML/)==5.1.1
* [pandas](https://pandas.pydata.org/)==0.24.2
* [scikit-learn](https://scikit-learn.org/stable/)==0.21.2
* [SQLAlchemy](https://www.sqlalchemy.org/)==1.3.5
* [tqdm](https://github.com/tqdm/tqdm)==4.32.1
* [xlrd](https://pypi.org/project/xlrd/)==1.2.0
* [python-dotenv](https://pypi.org/project/python-dotenv/)==0.10.3
* [psycopg2-binary](https://pypi.org/project/psycopg2-binary/)==2.8.3


## Installation des dépendances et package
1. `cd ~/WORK_DIR/pseudonymizer/src` : Aller dans le répertoire src du projet.
2. `python setup.py insall` : Installe le package pseudo-cli et ajoute le `pseudo-cli` disponible dans le PATH

## Execution du programme
 
[Lister les arguments dispo avec detail]


### Pseudonymisation
Par défault, le programme s'éxécute en mode "pseudo" qui permet d'obtenir les valeurs pseudonymisées pour chaque champs contenant de la donnée sensible ou personnel.

### Dépseudonymisation
`frfi-pseudo --reverse/-r` permet d'éxécuter le programme en mode "dépseudo" qui à partir des valeurs pseudonymisées d'un fichier, renvoie le fichier contenant les équivalents des valeurs en clair.

Retrouvez l'aide disponible avec la commande `frfi-pseudo --help`