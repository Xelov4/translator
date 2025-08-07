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

class AdvancedWebCrawler:
    def __init__(self, max_pages=50, max_depth=3):
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.visited_urls = set()
        self.pages_downloaded = 0
        self.setup_selenium()
        
    def setup_selenium(self):
        """Configurer Selenium pour les screenshots"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            print(f"Erreur lors de l'initialisation de Selenium: {e}")
            self.driver = None
        
    def take_screenshot(self, url, output_path):
        """Prendre un screenshot de la page d'accueil"""
        if not self.driver:
            return False
            
        try:
            self.driver.get(url)
            time.sleep(3)  # Attendre le chargement
            self.driver.save_screenshot(output_path)
            return True
        except Exception as e:
            print(f"Erreur lors du screenshot de {url}: {e}")
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
        if status_code in [200, 301, 302]:
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
        print(f"\n{'='*80}")
        print(f"CRAWLING: {base_url}")
        print(f"OUTIL: {tool_name}")
        print(f"{'='*80}")
        
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
                    
                    time.sleep(1)
                else:
                    print(f"❌ Impossible de récupérer: {current_url} (Code: {status_code})")
        else:
            print(f"❌ Page d'accueil inaccessible (Code: {status_code})")
        
        print(f"\n✅ Crawling terminé pour {base_url}")
        print(f"📁 Classification: {classification}")
        print(f"📁 Pages téléchargées: {self.pages_downloaded}")
        print(f"📂 Dossier: {output_dir}")
        
        return classification, self.pages_downloaded

def main():
    # Charger le fichier tools.csv
    print("Chargement du fichier tools.csv...")
    df = pd.read_csv('tools.csv')
    
    # Prendre les 10 premiers outils
    test_tools = df.head(10)
    print(f"Test sur {len(test_tools)} outils")
    
    # Créer le crawler
    crawler = AdvancedWebCrawler(max_pages=50, max_depth=3)
    
    # Statistiques
    stats = {"OK": 0, "ERROR": 0, "total_pages": 0}
    
    # Traiter chaque outil
    for index, row in test_tools.iterrows():
        tool_name = row['tool_name']
        tool_link = row['tool_link']
        
        if pd.isna(tool_link) or tool_link == '':
            print(f"⚠️ Pas d'URL pour {tool_name}")
            continue
        
        # Créer le nom du dossier (nettoyer les caractères spéciaux)
        folder_name = re.sub(r'[<>:"/\\|?*]', '_', tool_name)
        folder_name = re.sub(r'_+', '_', folder_name).strip('_')
        
        try:
            classification, pages_count = crawler.crawl_website(tool_link, folder_name)
            stats[classification] += 1
            stats["total_pages"] += pages_count
        except Exception as e:
            print(f"❌ Erreur lors du crawling de {tool_name}: {e}")
            stats["ERROR"] += 1
        
        print(f"\n{'='*80}")
        print(f"OUTIL TERMINÉ: {tool_name}")
        print(f"{'='*80}")
    
    # Fermer le driver Selenium
    if crawler.driver:
        crawler.driver.quit()
    
    # Afficher les statistiques finales
    print(f"\n{'='*80}")
    print("TEST DE CRAWLING AVANCÉ TERMINÉ")
    print(f"{'='*80}")
    print(f"📊 Statistiques:")
    print(f"  ✅ Sites OK: {stats['OK']}")
    print(f"  ❌ Sites en erreur: {stats['ERROR']}")
    print(f"  📄 Pages totales téléchargées: {stats['total_pages']}")
    print(f"📁 Structure des dossiers:")
    print(f"  - crawled_sites/OK/ (sites fonctionnels)")
    print(f"  - crawled_sites/ERROR/ (sites en erreur)")

if __name__ == "__main__":
    main() 