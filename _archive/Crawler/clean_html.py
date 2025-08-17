import os
import random
import re
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin, urlparse

def clean_html_content(html_content, base_url=""):
    """Nettoyer le contenu HTML en gardant seulement le texte et les liens, avec gestion des liens relatifs"""
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
    
    # Extraire les liens séparément avec gestion des liens relatifs
    links = []
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        link_text = link.get_text().strip()
        if href and link_text:
            # Convertir les liens relatifs en absolus
            if base_url and not href.startswith(('http://', 'https://', 'mailto:', 'tel:')):
                if href.startswith('/'):
                    # Lien relatif à la racine
                    absolute_url = urljoin(base_url, href)
                elif href.startswith('#'):
                    # Ancre de page
                    absolute_url = f"{base_url}{href}"
                else:
                    # Lien relatif au dossier courant
                    absolute_url = urljoin(base_url, href)
            else:
                absolute_url = href
            
            links.append(f"[{link_text}]({absolute_url})")
    
    # Combiner le texte et les liens
    result = text
    if links:
        result += "\n\n## Liens\n"
        result += "\n".join(links)
    
    return result

def extract_base_url_from_html(html_content):
    """Extraire l'URL de base depuis le contenu HTML"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Chercher dans les balises meta
    meta_url = soup.find('meta', property='og:url')
    if meta_url and meta_url.get('content'):
        return meta_url['content']
    
    # Chercher dans les liens canoniques
    canonical = soup.find('link', rel='canonical')
    if canonical and canonical.get('href'):
        return canonical['href']
    
    # Chercher dans les commentaires HTML (si ajouté par le crawler)
    comment_match = re.search(r'<!-- URL: (.+?) -->', html_content)
    if comment_match:
        return comment_match.group(1)
    
    return ""

def find_tool_directories():
    """Trouver tous les dossiers d'outils dans crawled_sites"""
    tool_dirs = []
    
    # Chercher dans les dossiers OK et ERROR
    for status_dir in ['crawled_sites/OK', 'crawled_sites/ERROR']:
        if os.path.exists(status_dir):
            for dir_name in os.listdir(status_dir):
                tool_path = os.path.join(status_dir, dir_name)
                if os.path.isdir(tool_path):
                    # Vérifier qu'il contient des fichiers HTML
                    html_files = [f for f in os.listdir(tool_path) if f.endswith('.html')]
                    if html_files:
                        tool_dirs.append({
                            'path': tool_path,
                            'name': dir_name,
                            'html_files': html_files,
                            'html_count': len(html_files)
                        })
    
    return tool_dirs

def process_tool_directory(tool_dir):
    """Traiter un dossier d'outil et créer un fichier Markdown fusionné"""
    tool_path = tool_dir['path']
    tool_name = tool_dir['name']
    html_files = tool_dir['html_files']
    
    print(f"🔄 Traitement de {tool_name} ({len(html_files)} fichiers HTML)")
    
    # Chemin du fichier Markdown de sortie
    md_path = os.path.join(tool_path, f"{tool_name}.md")
    
    # Contenu Markdown fusionné
    markdown_content = f"# {tool_name}\n\n"
    markdown_content += f"*Généré automatiquement à partir de {len(html_files)} fichiers HTML*\n\n"
    
    # Traiter chaque fichier HTML
    for i, html_file in enumerate(html_files, 1):
        html_path = os.path.join(tool_path, html_file)
        
    try:
        # Lire le fichier HTML
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
            # Extraire l'URL de base
            base_url = extract_base_url_from_html(html_content)
            
        # Nettoyer le contenu
            cleaned_content = clean_html_content(html_content, base_url)
            
            # Ajouter au contenu Markdown
            markdown_content += f"## {html_file.replace('.html', '')}\n\n"
            markdown_content += cleaned_content
            markdown_content += "\n\n---\n\n"
            
            print(f"  ✅ {html_file} traité")
            
        except Exception as e:
            print(f"  ❌ Erreur avec {html_file}: {str(e)}")
            markdown_content += f"## {html_file.replace('.html', '')}\n\n"
            markdown_content += f"*Erreur lors du traitement: {str(e)}*\n\n"
            markdown_content += "---\n\n"
    
    # Sauvegarder le fichier Markdown
    try:
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        # Calculer la taille
        md_size = len(markdown_content.encode('utf-8'))
        
        return {
            'tool_name': tool_name,
            'md_path': md_path,
            'html_count': len(html_files),
            'md_size': md_size,
            'status': 'success'
        }
        
    except Exception as e:
        return {
            'tool_name': tool_name,
            'error': str(e),
            'status': 'error'
        }

def main():
    """Fonction principale pour traiter TOUS les dossiers d'outils OK"""
    print("🧹 Script de fusion HTML -> Markdown - TOUS LES OUTILS OK")
    print("=" * 70)
    
    # Trouver tous les dossiers d'outils
    tool_dirs = find_tool_directories()
    
    if not tool_dirs:
        print("❌ Aucun dossier d'outil trouvé dans crawled_sites/")
        return
    
    # Filtrer seulement les outils OK
    ok_tools = [tool for tool in tool_dirs if 'OK' in tool['path']]
    
    if not ok_tools:
        print("❌ Aucun outil OK trouvé")
        return
    
    print(f"📁 Outils OK trouvés : {len(ok_tools)}")
    print(f"🎯 Traitement de TOUS les outils OK...")
    print()
    
    # Traiter chaque dossier avec progression
    results = []
    successful_count = 0
    error_count = 0
    
    for i, tool_dir in enumerate(ok_tools, 1):
        # Affichage de progression
        progress_percent = (i / len(ok_tools)) * 100
        print(f"🔄 [{i:4d}/{len(ok_tools):4d}] ({progress_percent:5.1f}%) Traitement de {tool_dir['name']}")
        
        result = process_tool_directory(tool_dir)
        results.append(result)
        
        if result['status'] == 'success':
            successful_count += 1
            print(f"    ✅ Fichier créé : {os.path.basename(result['md_path'])}")
            print(f"    📊 Taille : {result['md_size']:,} bytes")
            print(f"    📁 Fichiers HTML traités : {result['html_count']}")
        else:
            error_count += 1
            print(f"    ❌ Erreur : {result['error']}")
        
        # Afficher le résumé en cours
        print(f"    📈 Progression : {successful_count} ✅ | {error_count} ❌")
        print()
    
    # Résumé final
    print("📊 RÉSUMÉ FINAL")
    print("=" * 70)
    
    successful_results = [r for r in results if r['status'] == 'success']
    
    if successful_results:
        total_html_files = sum(r['html_count'] for r in successful_results)
        total_md_size = sum(r['md_size'] for r in successful_results)
        
        print(f"✅ Outils traités avec succès : {successful_count}/{len(ok_tools)} ({successful_count/len(ok_tools)*100:.1f}%)")
        print(f"❌ Outils en erreur : {error_count}/{len(ok_tools)} ({error_count/len(ok_tools)*100:.1f}%)")
        print(f"📁 Fichiers HTML traités au total : {total_html_files}")
        print(f"📁 Taille totale des fichiers Markdown : {total_md_size:,} bytes")
        print(f"📁 Taille moyenne par fichier : {total_md_size/len(successful_results):,.0f} bytes")
        
        # Statistiques par taille de fichier
        small_files = len([r for r in successful_results if r['md_size'] < 1000])
        medium_files = len([r for r in successful_results if 1000 <= r['md_size'] < 10000])
        large_files = len([r for r in successful_results if r['md_size'] >= 10000])
        
        print(f"\n📊 Répartition par taille :")
        print(f"   📄 Petits (< 1KB) : {small_files} fichiers")
        print(f"   📄 Moyens (1-10KB) : {medium_files} fichiers")
        print(f"   📄 Gros (≥ 10KB) : {large_files} fichiers")
        
        print(f"\n📂 Fichiers .md créés :")
        for result in successful_results:
            print(f"  - {result['md_path']}")
    
    if error_count > 0:
        print(f"\n❌ Erreurs détaillées :")
        for result in results:
            if result['status'] == 'error':
                print(f"  - {result['tool_name']} : {result['error']}")
    
    print(f"\n🎉 Traitement terminé ! {successful_count} fichiers Markdown créés avec succès.")

if __name__ == "__main__":
    main() 