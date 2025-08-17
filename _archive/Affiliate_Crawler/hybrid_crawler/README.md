# Crawler d'Affiliation Hybride

Ce crawler combine l'approche rapide de requests avec la robustesse de Selenium pour une détection optimale des programmes d'affiliation.

## Stratégie hybride

1. **Première tentative avec Requests**
   - Rapide et léger
   - Faible consommation de ressources
   - Parfait pour les sites simples

2. **Fallback vers Selenium**
   - Si requests échoue ou ne trouve pas d'affiliation
   - Support complet du JavaScript
   - Simulation de comportement humain
   - Anti-détection de bot

## Fonctionnalités

- Détection intelligente des programmes d'affiliation
- Approche en deux étapes pour optimiser les performances
- Extraction des emails de contact
- Gestion avancée des cookies et des sessions
- Rotation des User-Agents
- Simulation de comportement humain
- Masquage de l'automatisation
- Gestion des timeouts et des erreurs
- Sauvegarde de la progression
- Export des résultats en CSV

## Configuration

Le crawler accepte plusieurs paramètres :

- `--batch-size` : Taille des lots (défaut: 5000)
- `--max-pages` : Pages max par site (défaut: 10)
- `--max-concurrent` : Traitements simultanés (défaut: 3)
- `--memory-limit` : Limite mémoire MB (défaut: 1024)
- `--base-timeout` : Timeout base ms (défaut: 15000)
- `--requests-timeout` : Timeout requêtes s (défaut: 10)
- `--max-retries` : Tentatives max (défaut: 2)
- `--min-confidence` : Score minimum (défaut: 0.7)
- `--headless` : Mode headless (défaut: True)
- `--test` : Mode test
- `--debug` : Mode debug

## Prérequis

- Python 3.8+
- Chrome ou Chromium
- ChromeDriver (via webdriver_manager)

## Installation

```bash
pip install -r requirements.txt
```

## Utilisation

```bash
python affiliate_crawler_hybrid.py [options]
```

## Fichiers de sortie

- `affiliate_results_hybrid.csv` : Résultats
- `affiliate_progress_hybrid.json` : Progression
- `cookies/` : Stockage cookies
