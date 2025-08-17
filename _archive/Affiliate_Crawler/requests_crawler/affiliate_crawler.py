import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import time
import re
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from datetime import datetime
import csv
import logging

# Configuration du logging sans emojis pour éviter les problèmes d'encodage
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('affiliate_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class AffiliateCrawler:
    def __init__(self, max_pages=10, batch_size=5000):
        self.max_pages = max_pages
        self.batch_size = batch_size
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Fichiers de progression et résultats
        self.progress_file = "affiliate_progress.json"
        self.results_file = "affiliate_results.csv"
        self.progress_lock = threading.Lock()
        
        # Charger la progression existante
        self.load_progress()
        
        # Mots-clés multilingues pour les programmes d'affiliation
        self.affiliate_keywords = self._build_affiliate_keywords()
        
        # Regex avancées pour la détection
        self.affiliate_regexes = self._build_affiliate_regexes()
        
        # Regex pour les emails
        self.email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        # Initialiser le fichier CSV
        self.init_csv()
    
    def _build_affiliate_keywords(self):
        """Construire une liste exhaustive de mots-clés multilingues"""
        keywords = {
            'en': [
                'affiliate', 'affiliates', 'affiliation', 'partner', 'partners', 'partnership',
                'referral', 'referrals', 'referral program', 'referral system',
                'work with us', 'become a partner', 'join our program',
                'earn money', 'earn commission', 'commission program',
                'reseller', 'resellers', 'reseller program',
                'ambassador', 'ambassadors', 'ambassador program',
                'influencer', 'influencers', 'influencer program',
                'collaborate', 'collaboration', 'collaborator',
                'monetize', 'monetization', 'revenue sharing',
                'promote', 'promotion', 'promotional program',
                'marketing partner', 'business partner', 'strategic partner'
            ],
            'fr': [
                'affiliation', 'affiliés', 'partenaire', 'partenaires', 'partenariat',
                'parrainage', 'parrain', 'programme de parrainage',
                'travailler avec nous', 'devenir partenaire', 'rejoindre notre programme',
                'gagner de l\'argent', 'gagner une commission', 'programme de commission',
                'revendeur', 'revendeurs', 'programme de revendeur',
                'ambassadeur', 'ambassadeurs', 'programme d\'ambassadeur',
                'influenceur', 'influenceurs', 'programme d\'influenceur',
                'collaborer', 'collaboration', 'collaborateur',
                'monétiser', 'monétisation', 'partage des revenus',
                'promouvoir', 'promotion', 'programme promotionnel',
                'partenaire marketing', 'partenaire commercial', 'partenaire stratégique'
            ],
            'de': [
                'affiliate', 'affiliates', 'affiliation', 'partner', 'partner', 'partnerschaft',
                'empfehlung', 'empfehlungen', 'empfehlungsprogramm',
                'mit uns arbeiten', 'partner werden', 'unserem programm beitreten',
                'geld verdienen', 'provision verdienen', 'provisionsprogramm',
                'wiederverkäufer', 'wiederverkäufer', 'wiederverkäuferprogramm',
                'botschafter', 'botschafter', 'botschafterprogramm',
                'influencer', 'influencer', 'influencerprogramm',
                'zusammenarbeiten', 'zusammenarbeit', 'mitarbeiter',
                'monetarisieren', 'monetarisierung', 'umsatzbeteiligung',
                'fördern', 'förderung', 'förderprogramm',
                'marketingpartner', 'geschäftspartner', 'strategischer partner'
            ],
            'it': [
                'affiliazione', 'affiliati', 'partner', 'partner', 'partnership',
                'raccomandazione', 'raccomandazioni', 'programma di raccomandazione',
                'lavorare con noi', 'diventare partner', 'unirsi al nostro programma',
                'guadagnare denaro', 'guadagnare commissioni', 'programma di commissioni',
                'rivenditore', 'rivenditori', 'programma di rivenditore',
                'ambasciatore', 'ambasciatori', 'programma di ambasciatore',
                'influencer', 'influencer', 'programma di influencer',
                'collaborare', 'collaborazione', 'collaboratore',
                'monetizzare', 'monetizzazione', 'condivisione dei ricavi',
                'promuovere', 'promozione', 'programma promozionale',
                'partner di marketing', 'partner commerciale', 'partner strategico'
            ],
            'es': [
                'afiliación', 'afiliados', 'socio', 'socios', 'asociación',
                'recomendación', 'recomendaciones', 'programa de recomendación',
                'trabajar con nosotros', 'convertirse en socio', 'unirse a nuestro programa',
                'ganar dinero', 'ganar comisiones', 'programa de comisiones',
                'revendedor', 'revendedores', 'programa de revendedor',
                'embajador', 'embajadores', 'programa de embajador',
                'influencer', 'influencers', 'programa de influencer',
                'colaborar', 'colaboración', 'colaborador',
                'monetizar', 'monetización', 'compartir ingresos',
                'promocionar', 'promoción', 'programa promocional',
                'socio de marketing', 'socio comercial', 'socio estratégico'
            ]
        }
        
        # Ajouter des variations avec tirets, underscores, etc.
        extended_keywords = []
        for lang_keywords in keywords.values():
            for keyword in lang_keywords:
                extended_keywords.append(keyword)
                extended_keywords.append(keyword.replace(' ', '-'))
                extended_keywords.append(keyword.replace(' ', '_'))
                extended_keywords.append(keyword.replace(' ', ''))
        
        return extended_keywords
    
    def _build_affiliate_regexes(self):
        """Construire des regex avancées pour la détection"""
        patterns = [
            # Patterns pour les liens d'affiliation
            r'/(affiliate|partner|referral|reseller|ambassador|influencer)s?/?',
            r'/(become-?a-?partner|join-?our-?program|work-?with-?us)',
            r'/(earn-?money|earn-?commission|commission-?program)',
            r'/(collaborate|collaboration|collaborator)s?/?',
            r'/(monetize|monetization|revenue-?sharing)',
            r'/(promote|promotion|promotional-?program)',
            
            # Patterns pour les URLs contenant des mots-clés
            r'[^/]*(affiliate|partner|referral|reseller|ambassador|influencer)[^/]*',
            r'[^/]*(program|system|network|platform)[^/]*',
            
            # Patterns pour les paramètres d'URL
            r'[?&](ref|referral|affiliate|partner|source)=[^&]*',
            r'[?&](utm_source|utm_medium|utm_campaign)=[^&]*',
            
            # Patterns pour les ancres de liens
            r'<a[^>]*>(.*?(affiliate|partner|referral|reseller|ambassador|influencer).*?)</a>',
            r'<a[^>]*>(.*?(program|system|network|platform).*?)</a>',
            
            # Patterns pour le contenu de la page
            r'(affiliate|partner|referral|reseller|ambassador|influencer)\s+(program|system|network|platform)',
            r'(earn|make|get)\s+(money|commission|revenue|income)',
            r'(join|become|apply)\s+(for|to)\s+(our|the)\s+(program|system|network)'
        ]
        
        return [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    
    def init_csv(self):
        """Initialiser le fichier CSV avec les en-têtes"""
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
                    'date_analyse'
                ])
    
    def load_progress(self):
        """Charger le progrès depuis le fichier JSON"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    self.progress = json.load(f)
                logging.info(f"Progres charge: {len(self.progress.get('processed_tools', []))} outils traites")
            except Exception as e:
                logging.error(f"Erreur lors du chargement du progres: {e}")
                self.progress = {
                    "processed_tools": [],
                    "current_batch": 0,
                    "total_tools": 0,
                    "stats": {"OK": 0, "ERROR": 0, "AFFILIATE_FOUND": 0}
                }
        else:
            self.progress = {
                "processed_tools": [],
                "current_batch": 0,
                "total_tools": 0,
                "stats": {"OK": 0, "ERROR": 0, "AFFILIATE_FOUND": 0}
            }
    
    def save_progress(self):
        """Sauvegarder le progrès dans le fichier JSON"""
        with self.progress_lock:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
    
    def save_result(self, result_data):
        """Sauvegarder un résultat dans le CSV"""
        try:
            with open(self.results_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    result_data['nom_du_tool'],
                    result_data['url_root'],
                    result_data['code_reponse'],
                    result_data['programme_affiliation_trouve'],
                    result_data['lien_programme_affiliation'],
                    result_data['email_contact'],
                    result_data['pages_analysees'],
                    result_data['mots_cles_trouves'],
                    result_data['date_analyse']
                ])
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde du resultat: {e}")
    
    def detect_affiliate_program(self, html_content, base_url):
        """Détecter un programme d'affiliation dans le contenu HTML"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 1. Recherche dans les liens
            affiliate_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                link_text = link.get_text().lower()
                
                # Vérifier l'URL du lien
                for keyword in self.affiliate_keywords:
                    if keyword.lower() in href:
                        full_url = urljoin(base_url, href)
                        affiliate_links.append({
                            'url': full_url,
                            'text': link_text,
                            'type': 'url_keyword'
                        })
                        break
                
                # Vérifier le texte du lien
                for keyword in self.affiliate_keywords:
                    if keyword.lower() in link_text:
                        full_url = urljoin(base_url, href)
                        affiliate_links.append({
                            'url': full_url,
                            'text': link_text,
                            'type': 'text_keyword'
                        })
                        break
            
            # 2. Recherche avec les regex avancées
            for regex in self.affiliate_regexes:
                matches = regex.findall(html_content)
                if matches:
                    for match in matches:
                        if isinstance(match, tuple):
                            match_text = ' '.join(match)
                        else:
                            match_text = match
                        
                        # Chercher un lien proche de ce match
                        for link in soup.find_all('a', href=True):
                            if link.get_text().lower() in match_text.lower():
                                full_url = urljoin(base_url, link['href'])
                                affiliate_links.append({
                                    'url': full_url,
                                    'text': match_text,
                                    'type': 'regex_match'
                                })
            
            # 3. Recherche dans le contenu textuel
            text_content = soup.get_text().lower()
            found_keywords = []
            for keyword in self.affiliate_keywords:
                if keyword.lower() in text_content:
                    found_keywords.append(keyword)
            
            return affiliate_links, found_keywords
        except Exception as e:
            logging.error(f"Erreur lors de la detection d'affiliation: {e}")
            return [], []
    
    def extract_emails(self, html_content):
        """Extraire tous les emails du contenu HTML"""
        try:
            emails = self.email_regex.findall(html_content)
            # Filtrer les emails évidents (noreply, support génériques)
            filtered_emails = []
            for email in emails:
                email_lower = email.lower()
                if not any(spam in email_lower for spam in ['noreply', 'no-reply', 'donotreply']):
                    filtered_emails.append(email)
            return filtered_emails
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction des emails: {e}")
            return []
    
    def crawl_tool_for_affiliate(self, tool_name, tool_url):
        """Crawler un outil pour détecter son programme d'affiliation"""
        logging.info(f"Analyse de {tool_name} ({tool_url})")
        
        result_data = {
            'nom_du_tool': tool_name,
            'url_root': tool_url,
            'code_reponse': None,
            'programme_affiliation_trouve': 'non',
            'lien_programme_affiliation': '',
            'email_contact': '',
            'pages_analysees': 0,
            'mots_cles_trouves': '',
            'date_analyse': datetime.now().isoformat()
        }
        
        try:
            # 1. Tester la page d'accueil
            response = self.session.get(tool_url, timeout=15, allow_redirects=True)
            result_data['code_reponse'] = response.status_code
            
            if response.status_code != 200:
                logging.warning(f"{tool_name}: Page d'accueil inaccessible (Code: {response.status_code})")
                return result_data
            
            # 2. Analyser la page d'accueil
            affiliate_links, found_keywords = self.detect_affiliate_program(response.text, tool_url)
            emails = self.extract_emails(response.text)
            
            if affiliate_links:
                result_data['programme_affiliation_trouve'] = 'oui'
                result_data['lien_programme_affiliation'] = affiliate_links[0]['url']
                logging.info(f"{tool_name}: Programme d'affiliation trouve!")
            
            if emails:
                result_data['email_contact'] = emails[0]
            
            result_data['mots_cles_trouves'] = ', '.join(found_keywords)
            result_data['pages_analysees'] = 1
            
            # 3. Si pas trouvé, explorer les pages internes
            if result_data['programme_affiliation_trouve'] == 'non':
                affiliate_links, found_keywords, emails, pages_checked = self.explore_internal_pages(response.text, tool_url)
                
                if affiliate_links:
                    result_data['programme_affiliation_trouve'] = 'oui'
                    result_data['lien_programme_affiliation'] = affiliate_links[0]['url']
                    logging.info(f"{tool_name}: Programme d'affiliation trouve dans les pages internes!")
                
                if emails and not result_data['email_contact']:
                    result_data['email_contact'] = emails[0]
                
                result_data['mots_cles_trouves'] = ', '.join(found_keywords)
                result_data['pages_analysees'] = pages_checked
            
            logging.info(f"{tool_name}: Analyse terminee - Affiliation: {result_data['programme_affiliation_trouve']}")
            
        except Exception as e:
            logging.error(f"Erreur lors de l'analyse de {tool_name}: {e}")
            # Essayer de récupérer le code HTTP si possible
            if 'response' in locals() and hasattr(response, 'status_code'):
                result_data['code_reponse'] = response.status_code
            else:
                result_data['code_reponse'] = 'ERROR'
        
        return result_data
    
    def explore_internal_pages(self, homepage_html, base_url):
        """Explorer les pages internes pour trouver des programmes d'affiliation"""
        try:
            soup = BeautifulSoup(homepage_html, 'html.parser')
            
            # Extraire tous les liens internes
            internal_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                full_url = urljoin(base_url, href)
                
                # Vérifier que c'est un lien interne
                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                    internal_links.append(full_url)
            
            # Limiter le nombre de pages à explorer
            internal_links = internal_links[:self.max_pages - 1]  # -1 car on a déjà la page d'accueil
            
            affiliate_links = []
            found_keywords = []
            emails = []
            pages_checked = 1  # Page d'accueil déjà vérifiée
            
            for link_url in internal_links:
                try:
                    response = self.session.get(link_url, timeout=10)
                    if response.status_code == 200:
                        pages_checked += 1
                        
                        # Analyser cette page
                        page_affiliate_links, page_keywords = self.detect_affiliate_program(response.text, link_url)
                        page_emails = self.extract_emails(response.text)
                        
                        if page_affiliate_links:
                            affiliate_links.extend(page_affiliate_links)
                        
                        found_keywords.extend(page_keywords)
                        emails.extend(page_emails)
                        
                        # Si on a trouvé un programme d'affiliation, on peut arrêter
                        if affiliate_links:
                            break
                        
                        time.sleep(0.5)  # Pause entre les requêtes
                        
                except Exception as e:
                    logging.warning(f"Erreur lors de l'exploration de {link_url}: {e}")
                    continue
            
            # Dédupliquer les résultats
            affiliate_links = list({link['url']: link for link in affiliate_links}.values())
            found_keywords = list(set(found_keywords))
            emails = list(set(emails))
            
            return affiliate_links, found_keywords, emails, pages_checked
        except Exception as e:
            logging.error(f"Erreur lors de l'exploration des pages internes: {e}")
            return [], [], [], 1
    
    def process_batch(self, tools_batch):
        """Traiter un batch d'outils"""
        logging.info(f"Traitement du batch de {len(tools_batch)} outils")
        
        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_tool = {executor.submit(self.crawl_tool_for_affiliate, tool_name, tool_url): (tool_name, tool_url) 
                             for tool_name, tool_url in tools_batch}
            
            for future in as_completed(future_to_tool):
                tool_name, tool_url = future_to_tool[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Sauvegarder le résultat immédiatement
                    self.save_result(result)
                    
                    # Mettre à jour les statistiques
                    with self.progress_lock:
                        if result['programme_affiliation_trouve'] == 'oui':
                            self.progress['stats']['AFFILIATE_FOUND'] += 1
                        if result['code_reponse'] == 200:
                            self.progress['stats']['OK'] += 1
                        else:
                            self.progress['stats']['ERROR'] += 1
                        
                        self.progress['processed_tools'].append(tool_name)
                        self.save_progress()
                    
                    logging.info(f"{tool_name} traite - Affiliation: {result['programme_affiliation_trouve']}")
                    
                except Exception as e:
                    logging.error(f"Erreur lors du traitement de {tool_name}: {e}")
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Crawler specialise pour detecter les programmes d\'affiliation')
    parser.add_argument('--batch-size', type=int, default=5000, help='Taille des batches (defaut: 5000)')
    parser.add_argument('--max-pages', type=int, default=10, help='Nombre maximum de pages par site (defaut: 10)')
    parser.add_argument('--resume', action='store_true', help='Reprendre depuis le dernier batch')
    args = parser.parse_args()
    
    # Créer le crawler
    crawler = AffiliateCrawler(max_pages=args.max_pages, batch_size=args.batch_size)
    
    # Charger le fichier tools.csv
    logging.info("Chargement du fichier tools.csv...")
    try:
        df = pd.read_csv('tools.csv')
        all_tools = df[['tool_name', 'tool_link']].dropna()
        logging.info(f"Nombre total d'outils: {len(all_tools)}")
    except Exception as e:
        logging.error(f"Erreur lors du chargement de tools.csv: {e}")
        return
    
    # Déterminer le point de départ
    start_index = 0
    if args.resume and crawler.progress['current_batch'] > 0:
        start_index = crawler.progress['current_batch'] * args.batch_size
        logging.info(f"Reprise depuis l'index {start_index}")
    
    # Traiter par batches
    total_batches = (len(all_tools) - start_index + args.batch_size - 1) // args.batch_size
    
    for batch_num in range(crawler.progress['current_batch'], total_batches):
        start_idx = start_index + batch_num * args.batch_size
        end_idx = min(start_idx + args.batch_size, len(all_tools))
        
        batch_tools = []
        for idx in range(start_idx, end_idx):
            row = all_tools.iloc[idx]
            tool_name = row['tool_name']
            tool_url = row['tool_link']
            
            # Vérifier que l'outil n'a pas déjà été traité
            if tool_name not in crawler.progress['processed_tools']:
                batch_tools.append((tool_name, tool_url))
        
        if not batch_tools:
            logging.info(f"Batch {batch_num + 1}: Aucun nouvel outil a traiter")
            continue
        
        logging.info(f"Batch {batch_num + 1}/{total_batches}: {len(batch_tools)} outils a traiter")
        
        # Traiter le batch
        results = crawler.process_batch(batch_tools)
        
        # Mettre à jour le numéro de batch
        crawler.progress['current_batch'] = batch_num + 1
        crawler.save_progress()
        
        # Afficher les statistiques du batch
        affiliate_found = sum(1 for r in results if r['programme_affiliation_trouve'] == 'oui')
        logging.info(f"Batch {batch_num + 1} termine:")
        logging.info(f"  Outils traites: {len(results)}")
        logging.info(f"  Programmes d'affiliation trouves: {affiliate_found}")
        logging.info(f"  Total traite: {len(crawler.progress['processed_tools'])}")
        
        # Pause entre les batches
        if batch_num < total_batches - 1:
            logging.info("Pause de 5 secondes entre les batches...")
            time.sleep(5)
    
    # Statistiques finales
    logging.info(f"\n{'='*80}")
    logging.info("ANALYSE DES PROGRAMMES D'AFFILIATION TERMINEE")
    logging.info(f"{'='*80}")
    logging.info(f"Statistiques finales:")
    logging.info(f"  Outils traites: {len(crawler.progress['processed_tools'])}")
    logging.info(f"  Programmes d'affiliation trouves: {crawler.progress['stats']['AFFILIATE_FOUND']}")
    logging.info(f"  Erreurs: {crawler.progress['stats']['ERROR']}")
    logging.info(f"Fichier de resultats: {crawler.results_file}")
    logging.info(f"Fichier de progression: {crawler.progress_file}")

if __name__ == "__main__":
    main()
