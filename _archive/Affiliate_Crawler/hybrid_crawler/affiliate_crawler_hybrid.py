#!/usr/bin/env python3
"""
Crawler hybride pour détecter les programmes d'affiliation
Utilise requests/BeautifulSoup en premier, puis Playwright en fallback
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import json
import csv
import logging
import asyncio
import os
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from playwright.async_api import async_playwright
import urllib3
import socket

# Augmenter le timeout DNS global
socket.setdefaulttimeout(30)  # 30 secondes

# Configuration de requests
requests.adapters.DEFAULT_RETRIES = 3
session = requests.Session()
session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

# Désactiver les avertissements SSL pour requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('affiliate_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

@dataclass
class CrawlResult:
    """Structure pour stocker les résultats du crawl"""
    tool_name: str
    url_root: str
    status_code: str
    affiliate_found: bool
    affiliate_url: str
    emails: List[str]
    keywords_found: List[str]
    pages_checked: int
    method_used: str  # 'requests' ou 'playwright'

class AffiliateCrawlerHybrid:
    def __init__(self, max_pages=10, batch_size=5000, headless=True, max_workers=5):
        self.max_pages = max_pages
        self.batch_size = batch_size
        self.headless = headless
        self.max_workers = max_workers
        
        # Fichiers de résultats
        self.results_file = "affiliate_results_hybrid.csv"
        self.progress_file = "affiliate_progress_hybrid.json"
        
        # Headers pour requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        
        # Mots-clés et regex
        self.affiliate_keywords = self._build_affiliate_keywords()
        self.email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        # Session requests pour la réutilisation des connexions
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.verify = False  # Désactiver la vérification SSL
        
        # Initialiser les fichiers
        self._init_files()
        self.progress = self._load_progress()
        
        # Variables Playwright
        self.playwright = None
        self.browser = None
    
    def _build_affiliate_keywords(self) -> Dict[str, List[str]]:
        """Construire une liste exhaustive de mots-clés multilingues"""
        return {
            'url_patterns': [
                '/affiliate', '/affiliates', '/partners', '/referral', '/partnership',
                '/become-affiliate', '/become-partner', '/join-affiliate', '/join-partner',
                '/affiliate-program', '/partner-program', '/referral-program',
                '/affiliation', '/partenaires', '/programme-affiliation',
                '/afiliados', '/programa-afiliados', '/partnerprogramm'
            ],
            'content_keywords': {
                'en': [
                    'affiliate program', 'partner program', 'referral program',
                    'earn commission', 'commission rates', 'become an affiliate',
                    'join our program', 'affiliate partnership', 'revenue share',
                    'earn money', 'promote our', 'partner with us'
                ],
                'fr': [
                    'programme d\'affiliation', 'programme de partenariat',
                    'devenir partenaire', 'commission d\'affiliation',
                    'programme de parrainage', 'gagner une commission',
                    'rejoindre notre programme', 'partenariat affilié'
                ],
                'es': [
                    'programa de afiliados', 'programa de socios',
                    'hazte afiliado', 'comisiones de afiliados',
                    'únete a nuestro programa', 'gana comisiones'
                ],
                'de': [
                    'partnerprogramm', 'affiliate programm',
                    'partner werden', 'provision verdienen',
                    'jetzt partner werden', 'als partner registrieren'
                ]
            },
            'strong_indicators': [
                'commission rate', 'affiliate dashboard', 'partner portal',
                'affiliate login', 'partner login', 'affiliate signup',
                'commission structure', 'revenue share', 'payout rates',
                'programme d\'affiliation', 'programa de afiliados', 'partnerprogramm'
            ]
        }
    
    def _init_files(self):
        """Initialiser les fichiers CSV et JSON"""
        if not os.path.exists(self.results_file):
            with open(self.results_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    'nom_du_tool',
                    'url_root',
                    'code_reponse',
                    'programme_affiliation_trouve',
                    'lien_programme_affiliation',
                    'email_contact',
                    'pages_analysees',
                    'mots_cles_trouves',
                    'methode_utilisee',
                    'date_analyse'
                ])
    
    def _load_progress(self) -> Dict:
        """Charger la progression depuis le fichier JSON"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Erreur lors du chargement de la progression: {e}")
        
        return {
            "processed_tools": [],
            "stats": {
                "OK": 0,
                "ERROR": 0,
                "AFFILIATE_FOUND": 0,
                "REQUESTS_USED": 0,
                "PLAYWRIGHT_USED": 0
            }
        }
    
    def _save_progress(self):
        """Sauvegarder la progression dans le fichier JSON"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde de la progression: {e}")
    
    def _save_result(self, result: CrawlResult):
        """Sauvegarder un résultat dans le CSV"""
        try:
            with open(self.results_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    result.tool_name,
                    result.url_root,
                    result.status_code,
                    'oui' if result.affiliate_found else 'non',
                    result.affiliate_url or result.url_root,  # URL d'affiliation ou page d'accueil si non trouvé
                    '; '.join(result.emails[:3]) if result.emails else '',  # 3 premiers emails max
                    result.pages_checked,
                    ', '.join(result.keywords_found),
                    result.method_used,
                    datetime.now().isoformat()
                ])
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde du résultat: {e}")
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extraire les emails d'un texte"""
        return list(set(self.email_regex.findall(text)))
    
    def _check_affiliate_indicators(self, url: str, text: str) -> Tuple[bool, List[str]]:
        """Vérifier les indicateurs d'affiliation dans une URL et son contenu"""
        keywords_found = []
        
        # Vérifier l'URL
        url_lower = url.lower()
        for pattern in self.affiliate_keywords['url_patterns']:
            if pattern in url_lower:
                keywords_found.append(pattern)
        
        # Vérifier le contenu
        text_lower = text.lower()
        for lang_keywords in self.affiliate_keywords['content_keywords'].values():
            for keyword in lang_keywords:
                if keyword in text_lower:
                    keywords_found.append(keyword)
        
        # Vérifier les indicateurs forts
        has_strong_indicator = any(
            indicator in text_lower 
            for indicator in self.affiliate_keywords['strong_indicators']
        )
        
        return has_strong_indicator or len(keywords_found) >= 2, list(set(keywords_found))
    
    def _get_internal_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extraire les liens internes d'une page"""
        internal_links = []
        try:
            domain = urlparse(base_url).netloc
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                # Vérifier si c'est un lien interne
                if urlparse(full_url).netloc == domain:
                    internal_links.append(full_url)
        except Exception as e:
            logging.warning(f"Erreur lors de l'extraction des liens internes: {e}")
        
        return list(set(internal_links))[:self.max_pages]
    
    def _validate_url(self, url: str) -> Optional[str]:
        """Valider et normaliser une URL"""
        if not url or not isinstance(url, str) or url == 'nan':
            return None
            
        # Nettoyer l'URL
        url = url.strip()
        if not url:
            return None
            
        # Normaliser l'URL
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        # Vérifier le format de l'URL
        try:
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return None
            return url
        except Exception:
            return None
    
    def crawl_with_requests(self, tool_name: str, url: str) -> Optional[CrawlResult]:
        """Crawler un site avec requests/BeautifulSoup"""
        try:
            # Valider l'URL
            url = self._validate_url(url)
            if not url:
                logging.warning(f"{tool_name}: URL invalide: {url}")
                return None
            
            # Configuration des timeouts et retries
            retries = 3
            timeout = 20  # 20 secondes
            
            # Faire la requête initiale avec retries
            for attempt in range(retries):
                try:
                    response = self.session.get(
                        url,
                        timeout=timeout,
                        allow_redirects=True,
                        headers=self.headers
                    )
                    response.raise_for_status()
                    break  # Succès, sortir de la boucle
                except (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects) as e:
                    if attempt == retries - 1:  # Dernier essai
                        logging.warning(f"{tool_name}: Erreur de connexion après {retries} tentatives: {e}")
                        return None
                    logging.debug(f"{tool_name}: Tentative {attempt + 1}/{retries} échouée: {e}")
                    time.sleep(2)  # Attendre avant de réessayer
                except requests.HTTPError as e:
                    logging.warning(f"{tool_name}: Erreur HTTP {e.response.status_code}")
                    return None
                except Exception as e:
                    logging.warning(f"{tool_name}: Erreur inattendue: {e}")
                    return None
            
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Initialiser le résultat
                result = CrawlResult(
                    tool_name=tool_name,
                    url_root=url,
                    status_code=str(response.status_code),
                    affiliate_found=False,
                    affiliate_url='',
                    emails=self._extract_emails(response.text),
                    keywords_found=[],
                    pages_checked=1,
                    method_used='requests'
                )
                
                # Vérifier la page d'accueil
                is_affiliate, keywords = self._check_affiliate_indicators(url, response.text)
                result.keywords_found.extend(keywords)
                
                if is_affiliate:
                    result.affiliate_found = True
                    result.affiliate_url = url
                    return result
                
                # Explorer les liens internes
                internal_links = self._get_internal_links(soup, url)
                
                for link in internal_links:
                    try:
                        response = self.session.get(link, timeout=timeout)
                        response.raise_for_status()
                        
                        result.pages_checked += 1
                        result.emails.extend(self._extract_emails(response.text))
                        
                        is_affiliate, keywords = self._check_affiliate_indicators(
                            link, response.text
                        )
                        result.keywords_found.extend(keywords)
                        
                        if is_affiliate:
                            result.affiliate_found = True
                            result.affiliate_url = link
                            break
                            
                    except Exception as e:
                        logging.warning(f"Erreur lors de l'exploration de {link}: {e}")
                        continue
                
                # Dédupliquer les résultats
                result.emails = list(set(result.emails))
                result.keywords_found = list(set(result.keywords_found))
                
                return result
                
            except Exception as e:
                logging.error(f"Erreur lors de l'analyse de {tool_name}: {e}")
                return None
                
        except Exception as e:
            logging.error(f"Erreur avec requests pour {tool_name}: {e}")
            return None
    
    async def _init_playwright(self):
        """Initialiser Playwright"""
        if not self.browser:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu'
                ]
            )
    
    async def _close_playwright(self):
        """Fermer Playwright"""
        if self.browser:
            await self.browser.close()
            await self.playwright.stop()
            self.browser = None
            self.playwright = None
    
    async def crawl_with_playwright(self, tool_name: str, url: str) -> Optional[CrawlResult]:
        """Crawler un site avec Playwright (fallback)"""
        context = None
        page = None
        
        try:
            # Valider l'URL
            url = self._validate_url(url)
            if not url:
                logging.warning(f"{tool_name}: URL invalide pour Playwright: {url}")
                return None
            
            # Initialiser Playwright si nécessaire
            if not self.browser:
                await self._init_playwright()
            
            # Créer un contexte avec des paramètres optimisés
            context = await self.browser.new_context(
                viewport={'width': 1366, 'height': 768},
                user_agent=self.headers['User-Agent'],
                ignore_https_errors=True,
                java_script_enabled=True,
                bypass_csp=True,
                has_touch=False,
                is_mobile=False,
                device_scale_factor=1
            )
            
            # Créer une page
            page = await context.new_page()
            
            # Configurer les timeouts
            page.set_default_timeout(20000)
            page.set_default_navigation_timeout(25000)
            
            # Initialiser le résultat
            result = CrawlResult(
                tool_name=tool_name,
                url_root=url,
                status_code='ERROR',
                affiliate_found=False,
                affiliate_url='',
                emails=[],
                keywords_found=[],
                pages_checked=0,
                method_used='playwright'
            )
            
            # Charger la page avec retries
            retries = 2
            response = None
            
            for attempt in range(retries):
                try:
                    response = await page.goto(
                        url,
                        timeout=20000,
                        wait_until='domcontentloaded',
                        referer='https://www.google.com/'
                    )
                    
                    if response and response.status == 200:
                        result.status_code = str(response.status)
                        result.pages_checked += 1
                        break
                        
                except Exception as e:
                    if attempt == retries - 1:
                        logging.error(f"Erreur de chargement pour {tool_name}: {e}")
                        return result
                    logging.warning(f"Tentative {attempt + 1}/{retries} échouée: {e}")
                    await asyncio.sleep(2)
            
            if not response or response.status != 200:
                result.status_code = str(response.status if response else 'ERROR')
                return result
            
            try:
                # Extraire le contenu
                content = await page.content()
                text = await page.text_content('body')
                
                # Vérifier la page d'accueil
                result.emails = self._extract_emails(text)
                is_affiliate, keywords = self._check_affiliate_indicators(url, text)
                result.keywords_found.extend(keywords)
                
                if is_affiliate:
                    result.affiliate_found = True
                    result.affiliate_url = url
                    return result
                
                # Explorer les liens internes
                links = await page.query_selector_all('a[href]')
                internal_links = []
                
                for link in links:
                    try:
                        href = await link.get_attribute('href')
                        if href:
                            full_url = urljoin(url, href)
                            if urlparse(full_url).netloc == urlparse(url).netloc:
                                internal_links.append(full_url)
                    except Exception:
                        continue
                
                # Limiter le nombre de pages
                internal_links = list(set(internal_links))[:self.max_pages-1]
                
                # Explorer les pages internes
                for link in internal_links:
                    try:
                        await page.goto(link, timeout=10000, wait_until='domcontentloaded')
                        result.pages_checked += 1
                        
                        text = await page.text_content('body')
                        result.emails.extend(self._extract_emails(text))
                        
                        is_affiliate, keywords = self._check_affiliate_indicators(
                            link, text
                        )
                        result.keywords_found.extend(keywords)
                        
                        if is_affiliate:
                            result.affiliate_found = True
                            result.affiliate_url = link
                            break
                            
                    except Exception as e:
                        logging.warning(f"Erreur lors de l'exploration de {link}: {e}")
                        continue
                
                # Dédupliquer les résultats
                result.emails = list(set(result.emails))
                result.keywords_found = list(set(result.keywords_found))
                
                return result
                
            except Exception as e:
                logging.error(f"Erreur lors de l'analyse de {tool_name}: {e}")
                return result
                
        except Exception as e:
            logging.error(f"Erreur critique avec Playwright pour {tool_name}: {e}")
            return None
            
        finally:
            # Fermer la page et le contexte
            try:
                if page:
                    await page.close()
                if context:
                    await context.close()
            except Exception as e:
                logging.warning(f"Erreur lors de la fermeture des ressources Playwright: {e}")
    
    async def process_tool(self, tool_name: str, url: str) -> None:
        """Traiter un outil avec l'approche hybride"""
        try:
            print(f"\nTraitement de: {tool_name} ({url})")
            
            # Vérifier si déjà traité
            if tool_name in self.progress['processed_tools']:
                print(f"✓ {tool_name} déjà traité")
                return
            
            # Valider l'URL
            if not url or not isinstance(url, str) or url == 'nan':
                print(f"❌ {tool_name}: URL invalide")
                self.progress['stats']['ERROR'] += 1
                self.progress['processed_tools'].append(tool_name)
                self._save_progress()
                return
            
            # Essayer d'abord avec requests
            result = self.crawl_with_requests(tool_name, url)
            
            # Si requests échoue ou ne trouve rien, utiliser Playwright
            if not result or (not result.affiliate_found and result.status_code == '200'):
                print(f"⚡ {tool_name}: Tentative avec Playwright...")
                try:
                    result = await self.crawl_with_playwright(tool_name, url)
                except Exception as e:
                    logging.error(f"Erreur Playwright pour {tool_name}: {e}")
                    result = None
            
            if result:
                # Sauvegarder le résultat
                self._save_result(result)
                
                # Mettre à jour les statistiques
                self.progress['processed_tools'].append(tool_name)
                if result.affiliate_found:
                    self.progress['stats']['AFFILIATE_FOUND'] += 1
                if result.status_code == '200':
                    self.progress['stats']['OK'] += 1
                else:
                    self.progress['stats']['ERROR'] += 1
                
                if result.method_used == 'requests':
                    self.progress['stats']['REQUESTS_USED'] += 1
                else:
                    self.progress['stats']['PLAYWRIGHT_USED'] += 1
                
                self._save_progress()
                
                # Afficher le résultat
                status = '✅' if result.affiliate_found else '❌'
                print(f"{status} {tool_name}: Affiliation trouvée: {result.affiliate_found} ({result.method_used})")
            else:
                print(f"❌ {tool_name}: Erreur de traitement")
                self.progress['stats']['ERROR'] += 1
                self.progress['processed_tools'].append(tool_name)
                self._save_progress()
                
        except Exception as e:
            logging.error(f"Erreur lors du traitement de {tool_name}: {e}")
            print(f"❌ {tool_name}: Erreur: {str(e)[:100]}")
            self.progress['stats']['ERROR'] += 1
            self.progress['processed_tools'].append(tool_name)
            self._save_progress()
    
    async def run(self, tools_data: pd.DataFrame, is_test: bool = False):
        """Exécuter le crawler sur une liste d'outils"""
        try:
            # Filtrer les outils selon le mode
            if is_test:
                tools_to_process = tools_data.head(10)
                print(f"Mode TEST: {len(tools_to_process)} outils")
            else:
                tools_to_process = tools_data
                print(f"Mode PRODUCTION: {len(tools_to_process)} outils")
            
            # Créer la liste des outils à traiter
            tools_list = []
            for _, row in tools_to_process.iterrows():
                if row['tool_name'] not in self.progress['processed_tools']:
                    tools_list.append((row['tool_name'], row['tool_link']))
            
            if not tools_list:
                print("Aucun nouvel outil à traiter")
                return
            
            print(f"Outils à traiter: {len(tools_list)}")
            
            # Traiter par batches
            batch_size = min(self.batch_size, len(tools_list))
            total_batches = (len(tools_list) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(tools_list))
                batch = tools_list[start_idx:end_idx]
                
                print(f"\n{'='*80}")
                print(f"BATCH {batch_num + 1}/{total_batches}")
                print(f"{'='*80}")
                
                # Traiter le batch
                for i, (tool_name, url) in enumerate(batch):
                    print(f"[{i+1}/{len(batch)}] ", end='')
                    await self.process_tool(tool_name, url)
                
                # Afficher les statistiques du batch
                print(f"\nBatch {batch_num + 1} terminé:")
                print(f"  Outils traités: {len(self.progress['processed_tools'])}")
                print(f"  Affiliations trouvées: {self.progress['stats']['AFFILIATE_FOUND']}")
                print(f"  Requests utilisé: {self.progress['stats']['REQUESTS_USED']}")
                print(f"  Playwright utilisé: {self.progress['stats']['PLAYWRIGHT_USED']}")
                print(f"  Erreurs: {self.progress['stats']['ERROR']}")
                
                # Pause entre les batches
                if batch_num < total_batches - 1:
                    print("\nPause de 3 secondes...")
                    await asyncio.sleep(3)
            
            # Statistiques finales
            print(f"\n{'='*80}")
            print("🎉 TRAVAIL TERMINÉ ! 🎉")
            print(f"{'='*80}")
            print(f"📊 RÉSUMÉ FINAL:")
            print(f"  ✅ Outils traités: {len(self.progress['processed_tools'])}")
            print(f"  🎯 Affiliations trouvées: {self.progress['stats']['AFFILIATE_FOUND']}")
            print(f"  🚀 Requests utilisé: {self.progress['stats']['REQUESTS_USED']}")
            print(f"  🎭 Playwright utilisé: {self.progress['stats']['PLAYWRIGHT_USED']}")
            print(f"  ❌ Erreurs: {self.progress['stats']['ERROR']}")
            print(f"  📁 Résultats: {self.results_file}")
            print(f"  📋 Progression: {self.progress_file}")
            
        finally:
            # Fermer Playwright
            await self._close_playwright()

async def main():
    parser = argparse.ArgumentParser(description='Crawler hybride pour détecter les programmes d\'affiliation')
    parser.add_argument('--batch-size', type=int, default=5000, help='Taille des batches (défaut: 5000)')
    parser.add_argument('--max-pages', type=int, default=10, help='Pages max par site (défaut: 10)')
    parser.add_argument('--max-workers', type=int, default=5, help='Threads max (défaut: 5)')
    parser.add_argument('--headless', action='store_true', default=True, help='Mode headless')
    parser.add_argument('--test', action='store_true', help='Mode test (10 premiers outils)')
    parser.add_argument('--debug', action='store_true', help='Mode debug')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Charger les outils
        df = pd.read_csv('tools.csv')
        
        # Créer et exécuter le crawler
        crawler = AffiliateCrawlerHybrid(
            max_pages=args.max_pages,
            batch_size=args.batch_size,
            headless=args.headless,
            max_workers=args.max_workers
        )
        
        await crawler.run(df, is_test=args.test)
        
    except Exception as e:
        logging.error(f"Erreur générale: {e}")
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    asyncio.run(main())