# Crawler HTML

Ce crawler est conçu pour extraire et nettoyer le contenu HTML des sites web, avec un focus particulier sur la transformation en Markdown.

## Fonctionnalités

- Extraction du contenu HTML
- Nettoyage et formatage
- Conversion en Markdown
- Gestion des liens relatifs/absolus
- Fusion de fichiers HTML en un seul fichier Markdown
- Suivi de la progression

## Scripts principaux

### clean_html.py
Nettoie et convertit un fichier HTML en Markdown.

### clean_all_html.py
Traite tous les fichiers HTML d'un dossier.

### global_crawler.py
Crawler principal pour l'extraction de contenu.

### update_progress.py
Gestion de la progression du crawling.

## Fichiers de données

- `tools.csv` : Liste des outils à crawler
- `tool_translations.csv` : Traductions des outils

## Structure des dossiers

```
Crawler/
├── clean_html.py
├── clean_all_html.py
├── global_crawler.py
├── update_progress.py
├── tools.csv
├── tool_translations.csv
└── crawled_sites/
    └── [dossiers des sites]
```

## Utilisation

1. Configurer les outils dans `tools.csv`
2. Lancer le crawler global :
   ```bash
   python global_crawler.py
   ```
3. Nettoyer les fichiers HTML :
   ```bash
   python clean_all_html.py
   ```

## Format des fichiers CSV

### tools.csv
- Nom de l'outil
- URL
- Statut
- Autres métadonnées

### tool_translations.csv
- Nom de l'outil
- Traductions dans différentes langues
