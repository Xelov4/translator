# Crawler d'Affiliation avec Selenium

Ce crawler est une version avancée utilisant Selenium pour une détection plus robuste des programmes d'affiliation, capable de gérer le JavaScript et de simuler un comportement humain.

## Fonctionnalités

- Détection des programmes d'affiliation via analyse sémantique
- Support complet du JavaScript
- Simulation de comportement humain :
  - Scroll progressif
  - Mouvements de souris réalistes
  - Clics aléatoires
  - Délais naturels
- Masquage de l'automatisation :
  - Rotation des profils de navigateur
  - Headers personnalisés
  - Gestion des cookies
  - Émulation de périphériques
- Extraction des emails de contact
- Gestion des timeouts et des erreurs
- Sauvegarde de la progression
- Export des résultats en CSV

## Configuration

Le crawler accepte plusieurs paramètres pour ajuster son comportement :

- `--batch-size` : Taille des lots de traitement (défaut: 5000)
- `--max-pages` : Nombre maximum de pages par site (défaut: 10)
- `--max-concurrent` : Nombre de navigateurs simultanés (défaut: 3)
- `--memory-limit` : Limite de mémoire en MB (défaut: 1024)
- `--base-timeout` : Timeout de base en ms (défaut: 15000)
- `--requests-timeout` : Timeout des requêtes en secondes (défaut: 10)
- `--max-retries` : Nombre maximum de tentatives (défaut: 2)
- `--min-confidence` : Score minimum de confiance (défaut: 0.7)
- `--headless` : Mode headless (défaut: True)
- `--test` : Mode test (10 premiers outils)
- `--debug` : Mode debug avec plus de logs

## Prérequis

- Python 3.8+
- Chrome ou Chromium
- ChromeDriver (installé automatiquement via webdriver_manager)

## Installation

```bash
pip install -r requirements.txt
```

## Utilisation

```bash
python affiliate_crawler_v2.py [options]
```

## Fichiers de sortie

- `affiliate_results_v2.csv` : Résultats des analyses
- `affiliate_progress_v2.json` : Progression du traitement
- `cookies/` : Stockage des cookies par domaine

## Structure des résultats CSV

- `nom_du_tool` : Nom de l'outil analysé
- `url_root` : URL racine du site
- `code_reponse` : Code de réponse HTTP
- `programme_affiliation_trouve` : Oui/Non
- `lien_programme_affiliation` : URL du programme d'affiliation
- `email_contact` : Emails trouvés
- `pages_analysees` : Nombre de pages analysées
- `mots_cles_trouves` : Mots-clés détectés
- `methode_utilisee` : Méthode de détection utilisée
- `score_confiance` : Score de confiance (0-1)
- `date_analyse` : Date de l'analyse

## Gestion des ressources

Le crawler inclut une gestion intelligente des ressources :
- Pool de navigateurs avec limite configurable
- Surveillance de la mémoire et du CPU
- Nettoyage automatique des ressources
- Gestion des timeouts adaptatifs
