# Translator - Web Crawler & Data Enrichment Tool

## 📋 Description

Outil de crawling web intelligent pour enrichir les données d'outils AI. Le système analyse automatiquement les sites web, classe les résultats (OK/ERROR), et gère intelligemment la progression avec reprise automatique.

## 🚀 Fonctionnalités

### **Web Crawler Intelligent**
- **Classification automatique** : Sites OK vs ERROR basée sur les codes HTTP
- **Screenshots** : Capture automatique des pages d'accueil (1920x1080)
- **Nettoyage HTML** : Suppression du CSS pour alléger les fichiers
- **Multithreading** : Traitement parallèle configurable (2-4 workers)
- **Gestion d'erreurs** : Détails des erreurs avec codes de statut

### **Gestion de Progression**
- **Reprise automatique** : Reprend là où il s'est arrêté
- **Protection des sites OK** : Une fois validés, jamais re-testés
- **Re-test intelligent** : Option `--retest-errors` pour re-tester les échecs
- **Statistiques détaillées** : Suivi en temps réel avec estimations

### **Détails d'Erreur**
- **Codes HTTP** : 404, 403, 500, etc.
- **Types d'erreur** : Serveur, client, réseau, SSL
- **Messages descriptifs** : Explications claires des problèmes
- **Historique** : Date et heure des tests

## 📊 Statistiques Actuelles

- **✅ Sites OK** : 13,769
- **❌ Sites ERROR** : 2,993  
- **📄 Pages totales** : 39,668
- **📁 Dossiers** : 16,762 sites traités

## 🛠️ Installation

```bash
# Cloner le repository
git clone https://github.com/Xelov4/translator.git
cd translator

# Installer les dépendances
pip install pandas requests beautifulsoup4 selenium webdriver-manager pillow deep-translator
```

## 🚀 Utilisation

### **Crawler principal**
```bash
# Traiter tous les nouveaux outils
python global_crawler.py --max-workers 2 --max-pages 5

# Re-tester les sites en erreur
python global_crawler.py --retest-errors --max-workers 2 --max-pages 5

# Désactiver les screenshots pour plus de vitesse
python global_crawler.py --disable-screenshots --max-workers 4
```

### **Mise à jour des statistiques**
```bash
# Analyser les dossiers et mettre à jour crawler_progress.json
python update_progress.py
```

## 📁 Structure des Fichiers

```
translator/
├── global_crawler.py          # Crawler principal
├── update_progress.py         # Analyse des statistiques
├── tools.csv                  # Données source
├── crawler_progress.json      # Progression (auto-généré)
├── crawled_sites/
│   ├── OK/                   # Sites fonctionnels
│   └── ERROR/                # Sites en erreur
└── .gitignore               # Exclusions Git
```

## ⚙️ Options du Crawler

- `--retest-errors` : Re-tester uniquement les sites en erreur
- `--max-pages N` : Nombre maximum de pages par site (défaut: 5)
- `--max-workers N` : Nombre de workers parallèles (défaut: 2)
- `--disable-screenshots` : Désactiver les screenshots pour plus de vitesse

## 🔧 Configuration

Le système utilise un fichier `crawler_progress.json` pour :
- Suivre les sites traités
- Protéger les sites OK validés
- Gérer les détails d'erreur
- Calculer les statistiques

## 📈 Types d'Erreurs Détectées

- **404** : Page non trouvée
- **403** : Accès interdit
- **500** : Erreur serveur
- **SSL** : Problèmes de certificat
- **DNS** : Résolution de domaine échouée
- **Timeout** : Connexion expirée
- **Network** : Problèmes réseau

## 🤝 Contribution

1. Fork le projet
2. Créer une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit les changements (`git commit -m 'Add AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails. 