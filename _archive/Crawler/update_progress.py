import os
import json
import re
from datetime import datetime

def analyze_error_details(error_dir):
    """Analyser les détails d'erreur d'un site"""
    error_details = {
        "status_code": None,
        "error_type": "unknown",
        "error_message": "",
        "last_tested": None
    }
    
    # Chercher le fichier homepage.html pour extraire les détails
    homepage_file = os.path.join(error_dir, "homepage.html")
    if os.path.exists(homepage_file):
        try:
            with open(homepage_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Extraire le status code
            status_match = re.search(r'<!-- Status Code: (\d+) -->', content)
            if status_match:
                status_code = int(status_match.group(1))
                error_details["status_code"] = status_code
                
                # Classifier le type d'erreur
                if status_code >= 500:
                    error_details["error_type"] = "server_error"
                    error_details["error_message"] = f"Erreur serveur ({status_code})"
                elif status_code >= 400:
                    error_details["error_type"] = "client_error"
                    error_details["error_message"] = f"Erreur client ({status_code})"
                elif status_code >= 300:
                    error_details["error_type"] = "redirect"
                    error_details["error_message"] = f"Redirection ({status_code})"
                else:
                    error_details["error_type"] = "other"
                    error_details["error_message"] = f"Code {status_code}"
            
            # Extraire la date de test
            date_match = re.search(r'<!-- Téléchargé le: (.+?) -->', content)
            if date_match:
                error_details["last_tested"] = date_match.group(1)
                
        except Exception as e:
            error_details["error_message"] = f"Erreur de lecture: {str(e)}"
    
    return error_details

def count_pages_in_directory(directory):
    """Compter le nombre de pages HTML dans un répertoire"""
    if not os.path.exists(directory):
        return 0
    
    count = 0
    for file in os.listdir(directory):
        if file.endswith('.html'):
            count += 1
    return count

def update_crawler_progress():
    """Mettre à jour le fichier crawler_progress.json"""
    
    # Charger le fichier existant
    progress_file = "crawler_progress.json"
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    else:
        progress = {
            "processed_tools": [],
            "ok_tools": [],
            "error_tools": [],
            "stats": {"OK": 0, "ERROR": 0, "total_pages": 0},
            "error_details": {}
        }
    
    # Analyser les dossiers OK
    ok_dir = "crawled_sites/OK"
    ok_tools = []
    ok_pages = 0
    
    if os.path.exists(ok_dir):
        for tool_name in os.listdir(ok_dir):
            tool_dir = os.path.join(ok_dir, tool_name)
            if os.path.isdir(tool_dir):
                ok_tools.append(tool_name)
                ok_pages += count_pages_in_directory(tool_dir)
    
    # Analyser les dossiers ERROR
    error_dir = "crawled_sites/ERROR"
    error_tools = []
    error_details = {}
    
    if os.path.exists(error_dir):
        for tool_name in os.listdir(error_dir):
            tool_dir = os.path.join(error_dir, tool_name)
            if os.path.isdir(tool_dir):
                error_tools.append(tool_name)
                error_details[tool_name] = analyze_error_details(tool_dir)
    
    # Mettre à jour le fichier de progression
    progress["ok_tools"] = ok_tools
    progress["error_tools"] = error_tools
    progress["processed_tools"] = ok_tools + error_tools
    progress["stats"]["OK"] = len(ok_tools)
    progress["stats"]["ERROR"] = len(error_tools)
    progress["stats"]["total_pages"] = ok_pages
    progress["error_details"] = error_details
    progress["last_updated"] = datetime.now().isoformat()
    
    # Sauvegarder
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=2)
    
    # Afficher le résumé
    print(f"📊 Mise à jour du fichier crawler_progress.json terminée")
    print(f"✅ Sites OK: {len(ok_tools)}")
    print(f"❌ Sites ERROR: {len(error_tools)}")
    print(f"📄 Pages totales: {ok_pages}")
    
    # Afficher quelques exemples d'erreurs
    if error_details:
        print(f"\n🔍 Exemples d'erreurs détectées:")
        for tool, details in list(error_details.items())[:5]:
            print(f"  - {tool}: {details['error_type']} - {details['error_message']}")
    
    return progress

if __name__ == "__main__":
    update_crawler_progress() 