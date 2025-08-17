import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image
import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

class GlobalWebCrawler:
    def __init__(self, max_pages=5, max_depth=2, retest_errors=True, disable_screenshots=False):
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.retest_errors = retest_errors
        self.disable_screenshots = disable_screenshots
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.visited_urls = set()
        self.pages_downloaded = 0
        self.progress_file = "crawler_progress.json"
        self.load_progress()
        self.lock = threading.Lock()
        
    def load_progress(self):
        """Charger le progrès depuis le fichier JSON"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    self.progress = json.load(f)
                
                # Vérifier la compatibilité avec les anciens fichiers
                if 'ok_tools' not in self.progress:
                    self.progress['ok_tools'] = []
                if 'error_tools' not in self.progress:
                    self.progress['error_tools'] = []
                if 'error_details' not in self.progress:
                    self.progress['error_details'] = {}
                
                print(f"📋 Progrès chargé: {len(self.progress['processed_tools'])} outils traités")
                print(f"📊 Sites OK: {self.progress['stats']['OK']}, Sites ERROR: {self.progress['stats']['ERROR']}")
                print(f"✅ Sites validés définitivement: {len(self.progress['ok_tools'])}")
                print(f"❌ Sites en erreur: {len(self.progress['error_tools'])}")
                
            except Exception as e:
                print(f"Erreur lors du chargement du progrès: {e}")
                self.progress = {
                    "processed_tools": [], 
                    "ok_tools": [], 
                    "error_tools": [], 
                    "stats": {"OK": 0, "ERROR": 0, "total_pages": 0},
                    "error_details": {}
                }
        else:
            self.progress = {
                "processed_tools": [], 
                "ok_tools": [], 
                "error_tools": [], 
                "stats": {"OK": 0, "ERROR": 0, "total_pages": 0},
                "error_details": {}
            }
    
    def save_progress(self):
        """Sauvegarder le progrès dans le fichier JSON"""
        with self.lock:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        
    def setup_selenium(self):
        """Configurer Selenium pour les screenshots"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript")
        
        # Réduire les logs DevTools et erreurs
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--disable-dev-tools")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Variables d'environnement pour réduire les logs
        os.environ['WDM_LOG_LEVEL'] = '0'
        os.environ['WDM_PRINT_FIRST_LINE'] = 'False'
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            print(f"Erreur lors de l'initialisation de Selenium: {e}")
            return None
        
    def take_screenshot(self, url, output_path):
        """Prendre un screenshot de la page d'accueil"""
        if self.disable_screenshots:
            return True  # Simuler un succès si désactivé
            
        driver = self.setup_selenium()
        if not driver:
            return False
            
        try:
            driver.get(url)
            time.sleep(2)  # Réduire le temps d'attente
            driver.save_screenshot(output_path)
            driver.quit()
            return True
        except Exception as e:
            print(f"Erreur lors du screenshot de {url}: {e}")
            try:
                driver.quit()
            except:
                pass
            return False
    
    def clean_html(self, html_content):
        """Nettoyer le HTML en supprimant le CSS"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Supprimer les balises style
        for style in soup.find_all('style'):
            style.decompose()
        
        # Supprimer les attributs style
        for tag in soup.find_all(True):
            if tag.has_attr('style'):
                del tag['style']
        
        # Supprimer les liens vers les fichiers CSS
        for link in soup.find_all('link', rel='stylesheet'):
            link.decompose()
        
        return str(soup)
    
    def clean_filename(self, url):
        """Nettoyer l'URL pour créer un nom de fichier valide"""
        parsed = urlparse(url)
        path = parsed.path
        if not path or path == '/':
            path = 'homepage'
        else:
            path = path.strip('/').replace('/', '_')
        
        filename = re.sub(r'[<>:"/\\|?*]', '_', path)
        filename = re.sub(r'_+', '_', filename)
        filename = filename.strip('_')
        
        if not filename:
            filename = 'homepage'
            
        return f"{filename}.html"
    
    def get_page_content(self, url):
        """Récupérer le contenu d'une page avec gestion des redirections"""
        try:
            response = self.session.get(url, timeout=10, allow_redirects=True)
            return response.text, response.status_code, response.url
        except Exception as e:
            print(f"Erreur lors de la récupération de {url}: {e}")
            return None, None, None
    
    def classify_status_code(self, status_code):
        """Classifier le code de statut"""
        if status_code is None:
            return "ERROR"  # Si pas de status_code, c'est une erreur
        elif status_code in [200, 301, 302]:
            return "OK"
        elif status_code >= 400:
            return "ERROR"
        else:
            return "UNKNOWN"
    
    def extract_links(self, html, base_url):
        """Extraire tous les liens de la page"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                links.append(full_url)
        
        return links
    
    def crawl_website(self, base_url, tool_name):
        """Crawler un site web avec classification et screenshots"""
        print(f"\n{'='*60}")
        print(f"CRAWLING: {base_url}")
        print(f"OUTIL: {tool_name}")
        print(f"{'='*60}")
        
        # Tester d'abord la page d'accueil
        homepage_content, status_code, final_url = self.get_page_content(base_url)
        classification = self.classify_status_code(status_code)
        
        # Créer les dossiers de classification
        ok_dir = f"crawled_sites/OK/{tool_name}"
        error_dir = f"crawled_sites/ERROR/{tool_name}"
        
        if classification == "OK":
            output_dir = ok_dir
        else:
            output_dir = error_dir
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Prendre un screenshot de la page d'accueil
        screenshot_path = os.path.join(output_dir, "homepage_screenshot.png")
        screenshot_success = self.take_screenshot(base_url, screenshot_path)
        if screenshot_success:
            print(f"✓ Screenshot sauvegardé: homepage_screenshot.png")
        else:
            print(f"❌ Échec du screenshot")
        
        # Si la page d'accueil fonctionne, continuer le crawling
        if classification == "OK" and homepage_content:
            # Sauvegarder la page d'accueil nettoyée
            cleaned_html = self.clean_html(homepage_content)
            homepage_file = os.path.join(output_dir, "homepage.html")
            
            with open(homepage_file, 'w', encoding='utf-8') as f:
                f.write(f"<!-- URL: {base_url} -->\n")
                f.write(f"<!-- Status Code: {status_code} -->\n")
                f.write(f"<!-- Final URL: {final_url} -->\n")
                f.write(f"<!-- Téléchargé le: {datetime.now()} -->\n")
                f.write(cleaned_html)
            
            print(f"✓ Page d'accueil sauvegardée (nettoyée)")
            
            # Continuer le crawling des autres pages
            self.visited_urls = set()
            self.pages_downloaded = 1  # On a déjà la page d'accueil
            urls_to_visit = [(final_url, 0)]
            
            while urls_to_visit and self.pages_downloaded < self.max_pages:
                current_url, depth = urls_to_visit.pop(0)
                
                if current_url in self.visited_urls or depth > self.max_depth:
                    continue
                    
                print(f"\n[{self.pages_downloaded + 1}/{self.max_pages}] Profondeur {depth}: {current_url}")
                
                html_content, status_code, final_url = self.get_page_content(current_url)
                
                if html_content and status_code == 200:
                    # Nettoyer le HTML
                    cleaned_html = self.clean_html(html_content)
                    
                    # Créer le nom de fichier
                    filename = self.clean_filename(current_url)
                    filepath = os.path.join(output_dir, filename)
                    
                    # Sauvegarder la page
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(f"<!-- URL: {current_url} -->\n")
                        f.write(f"<!-- Profondeur: {depth} -->\n")
                        f.write(f"<!-- Status Code: {status_code} -->\n")
                        f.write(f"<!-- Téléchargé le: {datetime.now()} -->\n")
                        f.write(cleaned_html)
                    
                    print(f"✓ Sauvegardé: {filename}")
                    self.pages_downloaded += 1
                    self.visited_urls.add(current_url)
                    
                    # Extraire les nouveaux liens
                    if depth < self.max_depth:
                        new_links = self.extract_links(html_content, current_url)
                        for link in new_links:
                            if link not in self.visited_urls:
                                urls_to_visit.append((link, depth + 1))
                    
                    time.sleep(0.5)  # Réduire la pause
                else:
                    print(f"❌ Impossible de récupérer: {current_url} (Code: {status_code})")
        else:
            print(f"❌ Page d'accueil inaccessible (Code: {status_code})")
            
            # Enregistrer les détails d'erreur
            error_details = {
                "status_code": status_code,
                "error_type": self.get_error_type(status_code),
                "error_message": self.get_error_message(status_code),
                "last_tested": datetime.now().isoformat(),
                "url": base_url
            }
            
            # Sauvegarder les détails d'erreur
            error_details_file = os.path.join(output_dir, "error_details.json")
            with open(error_details_file, 'w', encoding='utf-8') as f:
                json.dump(error_details, f, indent=2)
        
        print(f"\n✅ Crawling terminé pour {base_url}")
        print(f"📁 Classification: {classification}")
        print(f"📁 Pages téléchargées: {self.pages_downloaded}")
        print(f"📂 Dossier: {output_dir}")
        
        return classification, self.pages_downloaded
    
    def get_error_type(self, status_code):
        """Déterminer le type d'erreur basé sur le code de statut"""
        if status_code is None:
            return "network_error"
        elif status_code >= 500:
            return "server_error"
        elif status_code >= 400:
            return "client_error"
        elif status_code >= 300:
            return "redirect_error"
        else:
            return "unknown_error"
    
    def get_error_message(self, status_code):
        """Obtenir un message d'erreur descriptif"""
        if status_code is None:
            return "Erreur réseau - Impossible de se connecter"
        elif status_code >= 500:
            return f"Erreur serveur ({status_code}) - Problème côté serveur"
        elif status_code == 404:
            return f"Page non trouvée ({status_code}) - URL invalide"
        elif status_code == 403:
            return f"Accès interdit ({status_code}) - Bloqué par le serveur"
        elif status_code >= 400:
            return f"Erreur client ({status_code}) - Problème de requête"
        elif status_code >= 300:
            return f"Redirection ({status_code}) - URL redirigée"
        else:
            return f"Code de statut inattendu: {status_code}"

    def process_tool(self, tool_data):
        """Traiter un outil (pour multithreading)"""
        tool_name, tool_link = tool_data
        
        if pd.isna(tool_link) or tool_link == '':
            return tool_name, "SKIP", 0
        
        # Créer le nom du dossier
        folder_name = re.sub(r'[<>:"/\\|?*]', '_', tool_name)
        folder_name = re.sub(r'_+', '_', folder_name).strip('_')
        
        try:
            classification, pages_count = self.crawl_website(tool_link, folder_name)
            return tool_name, classification, pages_count
        except Exception as e:
            print(f"❌ Erreur lors du crawling de {tool_name}: {e}")
            return tool_name, "ERROR", 0

def main():
    parser = argparse.ArgumentParser(description='Crawler global pour les outils')
    parser.add_argument('--retest-errors', action='store_true', help='Re-tester les sites en erreur')
    parser.add_argument('--max-pages', type=int, default=5, help='Nombre maximum de pages par site')
    parser.add_argument('--max-workers', type=int, default=2, help='Nombre de workers en parallèle')
    parser.add_argument('--disable-screenshots', action='store_true', help='Désactiver les screenshots pour éviter les erreurs Selenium')
    args = parser.parse_args()
    
    # Charger le fichier tools.csv
    print("Chargement du fichier tools.csv...")
    df = pd.read_csv('tools.csv')
    
    # Obtenir tous les outils
    all_tools = df[['tool_name', 'tool_link']].dropna()
    print(f"Nombre total d'outils à traiter: {len(all_tools)}")
    
    # Créer le crawler
    crawler = GlobalWebCrawler(max_pages=args.max_pages, max_depth=2, retest_errors=args.retest_errors, disable_screenshots=args.disable_screenshots)
    
    # Déterminer quels outils traiter
    tools_to_process = []
    
    if args.retest_errors:
        # Re-tester les sites en erreur
        print(f"🔄 Mode re-test des sites en erreur activé...")
        
        # Sauvegarder les sites OK actuels
        current_ok_tools = crawler.progress['ok_tools'].copy()
        current_ok_pages = crawler.progress['stats']['total_pages']
        
        print(f"✅ Sites OK protégés: {len(current_ok_tools)}")
        print(f"❌ Sites ERROR à re-tester: {len(crawler.progress['error_tools'])}")
        
        # Effacer les sites ERROR du fichier de progression
        crawler.progress['error_tools'] = []
        crawler.progress['error_details'] = {}
        crawler.progress['stats']['ERROR'] = 0
        
        # Restaurer les sites OK
        crawler.progress['ok_tools'] = current_ok_tools
        crawler.progress['stats']['total_pages'] = current_ok_pages
        
        # Sauvegarder immédiatement
        crawler.save_progress()
        
        # Créer une liste des sites qui sont dans ERROR mais pas dans OK
        error_sites = []
        for tool_name in crawler.progress['processed_tools']:
            # Vérifier si le dossier ERROR existe pour cet outil
            error_dir = f"crawled_sites/ERROR/{tool_name}"
            ok_dir = f"crawled_sites/OK/{tool_name}"
            
            if os.path.exists(error_dir) and not os.path.exists(ok_dir):
                error_sites.append(tool_name)
        
        print(f"🔄 {len(error_sites)} sites en erreur détectés pour re-test")
        
        for tool_name in error_sites:
            tool_row = all_tools[all_tools['tool_name'] == tool_name]
            if not tool_row.empty:
                tools_to_process.append((tool_name, tool_row.iloc[0]['tool_link']))
    else:
        # Traiter les nouveaux outils
        for index, row in all_tools.iterrows():
            tool_name = row['tool_name']
            tool_link = row['tool_link']
            
            if tool_name not in crawler.progress['processed_tools']:
                tools_to_process.append((tool_name, tool_link))
    
    print(f"📋 Outils à traiter: {len(tools_to_process)}")
    
    if not tools_to_process:
        print("✅ Tous les outils ont déjà été traités!")
        return
    
    # Traiter les outils avec multithreading
    start_time = time.time()
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Soumettre tous les outils
        future_to_tool = {executor.submit(crawler.process_tool, tool_data): tool_data for tool_data in tools_to_process}
        
        # Traiter les résultats au fur et à mesure
        for future in as_completed(future_to_tool):
            tool_name, classification, pages_count = future.result()
            processed_count += 1
            
            print(f"\n[{processed_count}/{len(tools_to_process)}] {tool_name}: {classification}")
            
            # Mettre à jour les statistiques
            with crawler.lock:
                if classification == "SKIP":
                    pass
                elif classification == "OK":
                    # Ajouter aux sites OK (définitivement validés)
                    if tool_name not in crawler.progress['ok_tools']:
                        crawler.progress['ok_tools'].append(tool_name)
                        crawler.progress['stats']['OK'] += 1
                        crawler.progress['stats']['total_pages'] += pages_count
                    
                    # Retirer des sites ERROR si présent
                    if tool_name in crawler.progress['error_tools']:
                        crawler.progress['error_tools'].remove(tool_name)
                        crawler.progress['stats']['ERROR'] -= 1
                        if tool_name in crawler.progress['error_details']:
                            del crawler.progress['error_details'][tool_name]
                    
                    # Ajouter aux outils traités si pas déjà présent
                    if tool_name not in crawler.progress['processed_tools']:
                        crawler.progress['processed_tools'].append(tool_name)
                else:
                    # Ajouter aux sites ERROR
                    if tool_name not in crawler.progress['error_tools']:
                        crawler.progress['error_tools'].append(tool_name)
                        crawler.progress['stats']['ERROR'] += 1
                    
                    # Ajouter aux outils traités si pas déjà présent
                    if tool_name not in crawler.progress['processed_tools']:
                        crawler.progress['processed_tools'].append(tool_name)
                
                crawler.save_progress()
            
            # Afficher la progression
            elapsed_time = time.time() - start_time
            if processed_count > 0:
                avg_time_per_tool = elapsed_time / processed_count
                remaining_tools = len(tools_to_process) - processed_count
                estimated_remaining_time = remaining_tools * avg_time_per_tool
                
                print(f"⏱️ Temps écoulé: {elapsed_time/60:.1f} min")
                print(f"⏱️ Temps restant estimé: {estimated_remaining_time/60:.1f} min")
                print(f"📊 Progression: {processed_count}/{len(tools_to_process)} ({processed_count/len(tools_to_process)*100:.1f}%)")
    
    # Afficher les statistiques finales
    total_time = time.time() - start_time
    print(f"\n{'='*80}")
    print("CRAWLING GLOBAL TERMINÉ")
    print(f"{'='*80}")
    print(f"📊 Statistiques finales:")
    print(f"  ✅ Sites OK: {crawler.progress['stats']['OK']}")
    print(f"  ❌ Sites en erreur: {crawler.progress['stats']['ERROR']}")
    print(f"  📄 Pages totales téléchargées: {crawler.progress['stats']['total_pages']}")
    print(f"⏱️ Temps total: {total_time/60:.1f} minutes")
    print(f"📁 Structure des dossiers:")
    print(f"  - crawled_sites/OK/ (sites fonctionnels)")
    print(f"  - crawled_sites/ERROR/ (sites en erreur)")
    print(f"📋 Fichier de progrès: {crawler.progress_file}")
    
    if crawler.progress['error_tools']:
        print(f"\n🔄 Sites en erreur disponibles pour re-test:")
        for tool in crawler.progress['error_tools'][:10]:  # Afficher les 10 premiers
            print(f"  - {tool}")
        if len(crawler.progress['error_tools']) > 10:
            print(f"  ... et {len(crawler.progress['error_tools']) - 10} autres")

if __name__ == "__main__":
    main() 