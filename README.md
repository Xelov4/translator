# Projet Translator - Analyse des Crawlers d'Affiliation

Ce projet contient différentes approches pour la détection des programmes d'affiliation sur les sites de logiciels SaaS/IA.

## Évolution des Crawlers

### 1. Crawler Requests (Version Initiale)
**Localisation**: `Affiliate_Crawler/requests_crawler/`

**Technologies**:
- requests
- BeautifulSoup4
- ThreadPoolExecutor

**Avantages**:
- Rapide
- Faible consommation de ressources
- Simple à maintenir

**Problèmes Rencontrés**:
- Blocage par les systèmes anti-bot (403 Forbidden)
- Ne peut pas exécuter le JavaScript
- Timeouts fréquents
- Erreurs SSL/TLS
- Problèmes de résolution DNS

### 2. Crawler Playwright (Version Abandonnée)
**Raison de l'Abandon**: `NotImplementedError` sur Windows avec Python 3.13

**Technologies**:
- Playwright
- asyncio

**Avantages**:
- Support complet du JavaScript
- API moderne et intuitive
- Bonnes capacités anti-détection

**Problèmes Rencontrés**:
- Incompatibilité avec Python 3.13 sur Windows
- Consommation mémoire élevée
- Crashes fréquents
- Lenteur relative
- Complexité de gestion des processus

### 3. Crawler Selenium (Version Stable)
**Localisation**: `Affiliate_Crawler/selenium_crawler/`

**Technologies**:
- Selenium 4
- webdriver-manager
- fake-useragent

**Avantages**:
- Stable et mature
- Bonne compatibilité
- Support du JavaScript
- Gestion avancée des cookies

**Problèmes Rencontrés**:
- Certains sites détectent toujours l'automatisation
- Erreurs de connexion WebDriver
- Lenteur relative
- Consommation de ressources importante

### 4. Crawler Hybride (Version Actuelle)
**Localisation**: `Affiliate_Crawler/hybrid_crawler/`

**Technologies**:
- requests + BeautifulSoup4 (première tentative)
- Selenium (fallback)
- fake-useragent
- ThreadPoolExecutor
- psutil

**Avantages**:
- Meilleur compromis vitesse/fiabilité
- Économie de ressources
- Robuste aux erreurs
- Gestion intelligente des cookies

**Problèmes Persistants**:
- Certains sites restent inaccessibles (403)
- Erreurs DNS occasionnelles
- Timeouts sur sites lents
- Détection d'automatisation résiduelle

## Défis Techniques Majeurs

### 1. Détection Anti-Bot
**Solutions Implémentées**:
- Rotation des User-Agents
- Gestion avancée des cookies
- Headers HTTP réalistes
- Délais aléatoires
- Simulation de comportement humain
- Masquage des signatures d'automatisation

### 2. Performance
**Solutions Implémentées**:
- Approche hybride (requests/Selenium)
- Pool de navigateurs
- Gestion de la mémoire
- Timeouts adaptatifs
- Retries exponentiels

### 3. Extraction de Données
**Solutions Implémentées**:
- Patterns multilingues
- Regex contextuels
- Validation des liens
- Détection d'emails
- Dédoublonnage par domaine

## Paramètres Configurables

### Paramètres Communs
```
--batch-size       : Taille des lots (défaut: 5000)
--max-pages       : Pages max par site (défaut: 10)
--max-concurrent  : Traitements simultanés (défaut: 3)
--memory-limit    : Limite mémoire MB (défaut: 1024)
--base-timeout    : Timeout base ms (défaut: 15000)
--requests-timeout: Timeout requêtes s (défaut: 10)
--max-retries     : Tentatives max (défaut: 2)
--min-confidence  : Score minimum (défaut: 0.7)
--headless        : Mode headless (défaut: True)
--test            : Mode test
--debug           : Mode debug
```

## Statistiques de Performance

### Crawler Requests
- Vitesse: ⭐⭐⭐⭐⭐ (Très rapide)
- Fiabilité: ⭐⭐ (Problèmes fréquents)
- Ressources: ⭐⭐⭐⭐⭐ (Très économe)
- Anti-Bot: ⭐ (Facilement détecté)

### Crawler Selenium
- Vitesse: ⭐⭐ (Lent)
- Fiabilité: ⭐⭐⭐⭐ (Stable)
- Ressources: ⭐⭐ (Gourmand)
- Anti-Bot: ⭐⭐⭐ (Moyennement détectable)

### Crawler Hybride
- Vitesse: ⭐⭐⭐⭐ (Rapide)
- Fiabilité: ⭐⭐⭐⭐ (Stable)
- Ressources: ⭐⭐⭐⭐ (Économe)
- Anti-Bot: ⭐⭐⭐⭐ (Bien camouflé)

## Recommandations pour le Futur

1. **Alternatives à Explorer**:
   - Puppeteer (alternative à Playwright)
   - pyppeteer (version Python de Puppeteer)
   - Scrapy (framework dédié au crawling)
   - Colly (en Go, pour la performance)

2. **Améliorations Possibles**:
   - Proxy rotation
   - Fingerprint randomization
   - WebSocket support
   - Distributed crawling
   - Cache intelligent
   - Meilleure gestion des captchas

3. **Points d'Attention**:
   - La détection anti-bot devient de plus en plus sophistiquée
   - Les sites utilisent de plus en plus de JavaScript
   - La performance reste un défi majeur
   - La gestion des ressources est critique

## Structure du Projet

```
Translator/
├── Affiliate_Crawler/           # Crawlers d'affiliation
│   ├── requests_crawler/        # Version requests pure
│   ├── selenium_crawler/        # Version Selenium
│   ├── hybrid_crawler/          # Version hybride (actuelle)
│   └── *.csv                    # Fichiers de résultats
└── Crawler/                     # Crawlers génériques
    ├── clean_html.py           # Nettoyage HTML
    ├── advanced_crawler.py     # Crawler principal
    └── tools.csv              # Liste des outils
```