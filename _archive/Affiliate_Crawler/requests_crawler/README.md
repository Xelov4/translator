# Crawler d'Affiliation avec Requests

Ce crawler est conçu pour détecter les programmes d'affiliation sur les sites web en utilisant principalement la bibliothèque `requests` pour une approche rapide et légère.

## Fonctionnalités

- Détection des programmes d'affiliation via analyse sémantique
- Extraction des emails de contact
- Gestion des cookies et des sessions
- Rotation des User-Agents
- Gestion des timeouts et des erreurs
- Sauvegarde de la progression
- Export des résultats en CSV

## Configuration

Le crawler accepte plusieurs paramètres pour ajuster son comportement :

- `--batch-size` : Taille des lots de traitement (défaut: 5000)
- `--max-pages` : Nombre maximum de pages par site (défaut: 10)
- `--max-concurrent` : Nombre de traitements simultanés (défaut: 3)
- `--memory-limit` : Limite de mémoire en MB (défaut: 1024)
- `--base-timeout` : Timeout de base en ms (défaut: 15000)
- `--requests-timeout` : Timeout des requêtes en secondes (défaut: 10)
- `--max-retries` : Nombre maximum de tentatives (défaut: 2)
- `--min-confidence` : Score minimum de confiance (défaut: 0.7)
- `--test` : Mode test (10 premiers outils)
- `--debug` : Mode debug avec plus de logs

## Utilisation

```bash
python affiliate_crawler.py [options]
```

## Fichiers de sortie

- `affiliate_results.csv` : Résultats des analyses
- `affiliate_progress.json` : Progression du traitement

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
