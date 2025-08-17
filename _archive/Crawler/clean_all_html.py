import os
import re
from bs4 import BeautifulSoup
from pathlib import Path
import time

def clean_html_content(html_content):
    """Nettoyer le contenu HTML en gardant seulement le texte et les liens"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Supprimer tous les éléments non désirés
    elements_to_remove = [
        'script', 'style', 'link', 'meta', 'noscript', 'iframe', 'embed',
        'object', 'applet', 'form', 'input', 'button', 'select', 'textarea',
        'img', 'video', 'audio', 'canvas', 'svg', 'picture', 'source',
        'track', 'map', 'area', 'figure', 'figcaption'
    ]
    
    for element in elements_to_remove:
        for tag in soup.find_all(element):
            tag.decompose()
    
    # Supprimer les commentaires HTML
    for comment in soup.find_all(string=lambda text: isinstance(text, str) and text.strip().startswith('<!--')):
        comment.extract()
    
    # Supprimer les attributs non essentiels de toutes les balises
    for tag in soup.find_all(True):
        # Garder seulement href pour les liens
        if tag.name == 'a':
            attrs_to_keep = ['href']
        else:
            attrs_to_keep = []
        
        # Supprimer tous les autres attributs
        for attr in list(tag.attrs.keys()):
            if attr not in attrs_to_keep:
                del tag[attr]
    
    # Supprimer les balises de structure non essentielles
    structure_tags_to_remove = [
        'div', 'span', 'section', 'article', 'aside', 'header', 'footer',
        'nav', 'main', 'container', 'wrapper', 'content', 'sidebar'
    ]
    
    for tag_name in structure_tags_to_remove:
        for tag in soup.find_all(tag_name):
            # Remplacer par son contenu
            tag.unwrap()
    
    # Nettoyer le texte
    text = soup.get_text()
    
    # Supprimer les espaces multiples et lignes vides
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = text.strip()
    
    # Extraire les liens séparément
    links = []
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        link_text = link.get_text().strip()
        if href and link_text:
            links.append(f"{link_text} -> {href}")
    
    # Combiner le texte et les liens
    result = text
    if links:
        result += "\n\n=== LIENS ===\n"
        result += "\n".join(links)
    
    return result

def find_all_html_files():
    """Trouver tous les fichiers HTML dans les dossiers crawled_sites"""
    html_files = []
    
    # Chercher dans les dossiers OK et ERROR
    for root, dirs, files in os.walk('crawled_sites'):
        for file in files:
            if file.endswith('.html'):
                html_files.append(os.path.join(root, file))
    
    return html_files

def process_html_file(html_path):
    """Traiter un fichier HTML et créer le fichier .txt correspondant"""
    try:
        # Lire le fichier HTML
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Nettoyer le contenu
        cleaned_content = clean_html_content(html_content)
        
        # Créer le chemin du fichier .txt
        txt_path = html_path.replace('.html', '.txt')
        
        # Sauvegarder le fichier .txt
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(cleaned_content)
        
        # Calculer la réduction de taille
        original_size = len(html_content)
        cleaned_size = len(cleaned_content)
        reduction = ((original_size - cleaned_size) / original_size) * 100
        
        return {
            'html_path': html_path,
            'txt_path': txt_path,
            'original_size': original_size,
            'cleaned_size': cleaned_size,
            'reduction_percent': reduction
        }
        
    except Exception as e:
        return {
            'html_path': html_path,
            'error': str(e)
        }

def main():
    """Fonction principale pour nettoyer tous les fichiers HTML"""
    print("🧹 Script de nettoyage HTML -> TXT (TOUS LES FICHIERS)")
    print("=" * 60)
    
    # Trouver tous les fichiers HTML
    html_files = find_all_html_files()
    
    if not html_files:
        print("❌ Aucun fichier HTML trouvé dans crawled_sites/")
        return
    
    print(f"📁 Fichiers HTML trouvés : {len(html_files)}")
    print(f"🔄 Début du traitement...")
    print()
    
    # Traiter chaque fichier
    results = []
    start_time = time.time()
    
    for i, html_file in enumerate(html_files, 1):
        if i % 100 == 0:  # Afficher le progrès tous les 100 fichiers
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            remaining = (len(html_files) - i) * avg_time
            print(f"📊 Progression : {i}/{len(html_files)} ({i/len(html_files)*100:.1f}%)")
            print(f"⏱️ Temps écoulé : {elapsed/60:.1f} min")
            print(f"⏱️ Temps restant estimé : {remaining/60:.1f} min")
            print()
        
        result = process_html_file(html_file)
        results.append(result)
    
    # Résumé final
    total_time = time.time() - start_time
    print("📊 RÉSUMÉ FINAL")
    print("=" * 60)
    
    successful_results = [r for r in results if 'error' not in r]
    error_results = [r for r in results if 'error' in r]
    
    if successful_results:
        total_original = sum(r['original_size'] for r in successful_results)
        total_cleaned = sum(r['cleaned_size'] for r in successful_results)
        avg_reduction = sum(r['reduction_percent'] for r in successful_results) / len(successful_results)
        
        print(f"✅ Fichiers traités avec succès : {len(successful_results)}/{len(html_files)}")
        print(f"📁 Taille totale originale : {total_original:,} bytes ({total_original/1024/1024:.1f} MB)")
        print(f"📁 Taille totale nettoyée : {total_cleaned:,} bytes ({total_cleaned/1024/1024:.1f} MB)")
        print(f"📉 Réduction moyenne : {avg_reduction:.1f}%")
        print(f"💾 Espace économisé : {total_original - total_cleaned:,} bytes ({(total_original - total_cleaned)/1024/1024:.1f} MB)")
        print(f"⏱️ Temps total : {total_time/60:.1f} minutes")
        
        # Statistiques par dossier
        ok_files = [r for r in successful_results if 'crawled_sites/OK/' in r['html_path']]
        error_files = [r for r in successful_results if 'crawled_sites/ERROR/' in r['html_path']]
        
        print(f"\n📂 Répartition :")
        print(f"  - Sites OK : {len(ok_files)} fichiers")
        print(f"  - Sites ERROR : {len(error_files)} fichiers")
    
    if error_results:
        print(f"\n❌ Erreurs : {len(error_results)} fichier(s)")
        for result in error_results[:5]:  # Afficher seulement les 5 premières erreurs
            print(f"  - {result['html_path']} : {result['error']}")
        if len(error_results) > 5:
            print(f"  ... et {len(error_results) - 5} autres erreurs")

if __name__ == "__main__":
    main() 