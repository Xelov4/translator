#!/usr/bin/env python3
"""
Crawler hybride optimisé pour détecter les programmes d'affiliation
Version 2.0 avec gestion des ressources améliorée et détection contextuelle
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
import psutil
import signal
import platform
from datetime import datetime
from urllib.parse import urljoin, urlparse
import argparse
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import urllib3
import socket
from collections import deque
import warnings
import atexit
import sys
import random
import string
import re
import time
from fake_useragent import UserAgent
from http.cookiejar import CookieJar
from typing import Dict, Optional, List, Set, Tuple, Any
from dataclasses import dataclass, field, asdict
import json
from urllib.parse import urlparse
from datetime import datetime, timedelta

# Ignorer les avertissements ResourceWarning pour les pipes
warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*")

# Configuration spécifique Windows pour asyncio
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configuration globale
socket.setdefaulttimeout(30)  # Timeout DNS global
requests.adapters.DEFAULT_RETRIES = 3  # Retries pour requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # Désactiver les warnings SSL

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('affiliate_crawler_v2.log', encoding='utf-8'),
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
    method_used: str
    confidence_score: float

@dataclass
class Cookie:
    """Représente un cookie avec ses attributs"""
    name: str
    value: str
    domain: str
    path: str = "/"
    secure: bool = True
    httpOnly: bool = True
    sameSite: str = "Lax"
    expires: Optional[int] = None

@dataclass
class BrowserProfile:
    """Profil de navigateur pour simuler un comportement humain"""
    user_agent: str
    accept_language: str
    platform: str
    vendor: str
    cookies: Dict[str, Cookie] = field(default_factory=dict)
    viewport: Dict[str, int] = field(default_factory=lambda: {"width": 1920, "height": 1080})
    screen: Dict[str, int] = field(default_factory=lambda: {"width": 1920, "height": 1080})
    timezone: str = "Europe/Paris"
    webgl_vendor: str = "Google Inc. (NVIDIA)"
    webgl_renderer: str = "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)"
    audio: bool = True
    touch: bool = False
    mobile: bool = False
    
    def add_cookie(self, cookie: Cookie) -> None:
        """Ajoute un cookie au profil"""
        self.cookies[cookie.name] = cookie
    
    def get_cookies_for_domain(self, domain: str) -> List[Cookie]:
        """Récupère les cookies pour un domaine donné"""
        return [
            cookie for cookie in self.cookies.values()
            if cookie.domain == domain or domain.endswith(f".{cookie.domain}")
        ]
    
    def to_selenium_cookies(self, domain: str) -> List[Dict[str, Any]]:
        """Convertit les cookies en format Selenium"""
        cookies = []
        for cookie in self.get_cookies_for_domain(domain):
            cookie_dict = {
                'name': cookie.name,
                'value': cookie.value,
                'domain': cookie.domain,
                'path': cookie.path,
                'secure': cookie.secure,
                'httpOnly': cookie.httpOnly
            }
            if cookie.expires:
                cookie_dict['expiry'] = cookie.expires
            cookies.append(cookie_dict)
        return cookies

class BrowserProfileManager:
    """Gestionnaire de profils de navigateur"""
    def __init__(self):
        self.ua = UserAgent(browsers=['chrome', 'firefox', 'edge'])
        self.used_profiles: Set[str] = set()
        self.common_languages = [
            'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7',
            'en-GB,en;q=0.9,fr-FR;q=0.8,fr;q=0.7',
            'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7'
        ]
        self.common_platforms = [
            'Windows NT 10.0; Win64; x64',
            'Macintosh; Intel Mac OS X 10_15_7',
            'X11; Linux x86_64'
        ]
        self.common_vendors = [
            'Google Inc.',
            'Apple Computer, Inc.',
            'Mozilla Foundation'
        ]
    
    def generate_profile(self) -> BrowserProfile:
        """Génère un profil de navigateur aléatoire mais cohérent"""
        ua = self.ua.random
        platform = random.choice(self.common_platforms)
        vendor = random.choice(self.common_vendors)
        lang = random.choice(self.common_languages)
        
        # Générer un fingerprint unique
        fingerprint = f"{ua}{platform}{vendor}{lang}"
        while fingerprint in self.used_profiles:
            ua = self.ua.random
            platform = random.choice(self.common_platforms)
            vendor = random.choice(self.common_vendors)
            lang = random.choice(self.common_languages)
            fingerprint = f"{ua}{platform}{vendor}{lang}"
        
        self.used_profiles.add(fingerprint)
        
        return BrowserProfile(
            user_agent=ua,
            accept_language=lang,
            platform=platform,
            vendor=vendor
        )
    
    def get_headers(self, profile: BrowserProfile) -> Dict[str, str]:
        """Génère des headers HTTP cohérents pour le profil"""
        # Headers de base
        headers = {
            'User-Agent': profile.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': profile.accept_language,
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'DNT': '1',
            'Sec-CH-UA': f'"{profile.vendor}";v="{random.randint(90, 120)}", "Chromium";v="{random.randint(90, 120)}"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': f'"{profile.platform}"',
            'Sec-CH-UA-Arch': '"x86"',
            'Sec-CH-UA-Full-Version': f'"{random.randint(90, 120)}.0.{random.randint(1000, 9999)}.{random.randint(100, 999)}"',
            'Sec-CH-UA-Platform-Version': '"10.0.0"',
            'Sec-CH-UA-Model': '""',
            'Sec-CH-Prefers-Color-Scheme': random.choice(['light', 'dark']),
            'Sec-CH-UA-Bitness': '"64"',
            'Priority': 'u=0, i',
            'Cache-Control': 'max-age=0'
        }
        
        # Ajouter des headers aléatoires
        if random.random() < 0.3:  # 30% de chance
            headers['If-None-Match'] = f'W/"{"%016x" % random.randrange(16**16)}"'
        
        if random.random() < 0.3:  # 30% de chance
            headers['If-Modified-Since'] = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%a, %d %b %Y %H:%M:%S GMT')
        
        return headers

class CookieManager:
    """Gestionnaire de cookies"""
    def __init__(self, storage_dir: str = "cookies"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self.common_cookies = {
            'gdpr': Cookie(
                name='gdpr_consent',
                value='accepted',
                domain='',
                expires=int(time.time()) + 365 * 24 * 60 * 60
            ),
            'language': Cookie(
                name='preferred_language',
                value='en-US',
                domain='',
                expires=int(time.time()) + 30 * 24 * 60 * 60
            ),
            'timezone': Cookie(
                name='timezone',
                value='Europe/Paris',
                domain='',
                expires=int(time.time()) + 30 * 24 * 60 * 60
            )
        }
    
    def _get_storage_path(self, domain: str) -> str:
        """Obtient le chemin de stockage pour un domaine"""
        safe_domain = re.sub(r'[^\w\-_.]', '_', domain)
        return os.path.join(self.storage_dir, f"{safe_domain}.json")
    
    def load_cookies(self, domain: str) -> List[Cookie]:
        """Charge les cookies pour un domaine"""
        try:
            path = self._get_storage_path(domain)
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                    return [Cookie(**cookie) for cookie in data]
        except Exception as e:
            logging.warning(f"Erreur chargement cookies pour {domain}: {e}")
        return []
    
    def save_cookies(self, domain: str, cookies: List[Cookie]) -> None:
        """Sauvegarde les cookies pour un domaine"""
        try:
            path = self._get_storage_path(domain)
            with open(path, 'w') as f:
                json.dump([asdict(cookie) for cookie in cookies], f)
        except Exception as e:
            logging.warning(f"Erreur sauvegarde cookies pour {domain}: {e}")
    
    def extract_cookies_from_selenium(self, driver) -> List[Cookie]:
        """Extrait les cookies depuis Selenium"""
        cookies = []
        for cookie in driver.get_cookies():
            try:
                cookies.append(Cookie(
                    name=cookie['name'],
                    value=cookie['value'],
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/'),
                    secure=cookie.get('secure', True),
                    httpOnly=cookie.get('httpOnly', True),
                    expires=cookie.get('expiry')
                ))
            except Exception as e:
                logging.warning(f"Erreur extraction cookie: {e}")
        return cookies
    
    def generate_cookies(self, domain: str) -> List[Cookie]:
        """Génère des cookies réalistes pour un domaine"""
        cookies = []
        
        # Ajouter les cookies communs
        for cookie in self.common_cookies.values():
            cookie_copy = Cookie(
                name=cookie.name,
                value=cookie.value,
                domain=domain,
                path=cookie.path,
                secure=cookie.secure,
                httpOnly=cookie.httpOnly,
                expires=cookie.expires
            )
            cookies.append(cookie_copy)
        
        # Ajouter des cookies spécifiques au site
        site_cookies = {
            'visitor_id': f"v{random.randint(100000, 999999)}",
            'session_id': f"s{random.randint(100000, 999999)}",
            'first_visit': int(time.time()) - random.randint(0, 30 * 24 * 60 * 60)
        }
        
        for name, value in site_cookies.items():
            cookies.append(Cookie(
                name=name,
                value=str(value),
                domain=domain,
                expires=int(time.time()) + random.randint(1, 7) * 24 * 60 * 60
            ))
        
        return cookies

class ResourceManager:
    """Gestionnaire de ressources"""
    def __init__(self, memory_limit_mb: int = 1024):
        self.memory_limit = memory_limit_mb * 1024 * 1024
        self.process = psutil.Process(os.getpid())
        self.browser_manager = BrowserProfileManager()
        self.cookie_manager = CookieManager()
        self.delay_min = 2
        self.delay_max = 5
    
    def check_memory(self) -> bool:
        return self.process.memory_info().rss < self.memory_limit
    
    def get_memory_usage(self) -> float:
        return self.process.memory_info().rss / (1024 * 1024)
    
    def get_cpu_usage(self) -> float:
        return self.process.cpu_percent()
    
    def get_random_delay(self) -> float:
        """Génère un délai aléatoire entre les requêtes"""
        base_delay = random.uniform(self.delay_min, self.delay_max)
        # Ajouter un petit délai aléatoire supplémentaire
        jitter = random.uniform(0, 0.5)
        return base_delay + jitter
    
    def save_cookies(self, domain: str, cookies: Dict[str, str]):
        """Sauvegarde les cookies pour un domaine"""
        try:
            with open(f"cookies_{domain}.json", "w") as f:
                json.dump(cookies, f)
        except Exception:
            pass
    
    def load_cookies(self, domain: str) -> Dict[str, str]:
        """Charge les cookies pour un domaine"""
        try:
            with open(f"cookies_{domain}.json", "r") as f:
                return json.load(f)
        except Exception:
            return {}

class BrowserPool:
    """Pool de navigateurs Selenium"""
    def __init__(self, max_size: int = 3, headless: bool = True):
        self.max_size = max_size
        self.headless = headless
        self.browsers = deque(maxlen=max_size)
        self.semaphore = asyncio.Semaphore(max_size)
        self.profile_manager = BrowserProfileManager()
        self.current_profile = None
        self.resource_manager = None  # Sera défini par AffiliateCrawler
    
    def _setup_options(self, profile: BrowserProfile) -> Options:
        """Configurer les options Chrome avec un profil spécifique"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Arguments de base pour la stabilité
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Configuration du profil
        options.add_argument(f'--user-agent={profile.user_agent}')
        options.add_argument(f'--lang={profile.accept_language.split(",")[0]}')
        options.add_argument(f'--window-size={profile.viewport["width"]},{profile.viewport["height"]}')
        
        # Paramètres avancés pour éviter la détection
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Emuler des capacités de navigateur réalistes
        options.add_argument('--enable-javascript')
        if profile.audio:
            options.add_argument('--enable-audio-output')
        else:
            options.add_argument('--mute-audio')
        
        # Configuration des préférences
        prefs = {
            'profile.default_content_setting_values': {
                'notifications': 2,  # Bloquer les notifications
                'plugins': 2,  # Bloquer les plugins
            },
            'profile.managed_default_content_settings': {
                'javascript': 1,  # Activer JavaScript
                'images': 1,  # Charger les images
                'cookies': 1,  # Accepter les cookies
            },
            'profile.password_manager_enabled': False,
            'credentials_enable_service': False,
            'profile.cookie_controls_mode': 0,
            'intl.accept_languages': profile.accept_language
        }
        options.add_experimental_option('prefs', prefs)
        
        return options
    
    def _setup_cdp_commands(self, driver) -> None:
        """Configure les commandes CDP pour masquer l'automatisation"""
        if not self.current_profile:
            return
            
        # Masquer les indicateurs d'automatisation de manière plus sophistiquée
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                // Masquer webdriver
                delete Object.getPrototypeOf(navigator).webdriver;
                
                // Simuler des plugins réalistes
                Object.defineProperty(navigator, 'plugins', {
                    get: () => {
                        const fakePlugins = new Array(5).fill(null).map(() => ({
                            name: ['Chrome PDF Plugin', 'Chrome PDF Viewer', 'Native Client'][Math.floor(Math.random() * 3)],
                            filename: ['internal-pdf-viewer', 'mhjfbmdgcfjbbpaeojofohoefgiehjai', 'internal-nacl-plugin'][Math.floor(Math.random() * 3)],
                            description: ['Portable Document Format', 'Chrome PDF Viewer', 'Native Client Module'][Math.floor(Math.random() * 3)]
                        }));
                        return Object.setPrototypeOf(fakePlugins, PluginArray.prototype);
                    }
                });
                
                // Simuler des langues réalistes
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['fr-FR', 'fr', 'en-US', 'en', 'de-DE', 'es-ES'].slice(0, 2 + Math.floor(Math.random() * 3))
                });
                
                // Simuler un objet chrome réaliste
                window.chrome = {
                    app: {
                        isInstalled: false,
                        InstallState: {
                            DISABLED: 'disabled',
                            INSTALLED: 'installed',
                            NOT_INSTALLED: 'not_installed'
                        },
                        RunningState: {
                            CANNOT_RUN: 'cannot_run',
                            READY_TO_RUN: 'ready_to_run',
                            RUNNING: 'running'
                        }
                    },
                    runtime: {
                        OnInstalledReason: {
                            CHROME_UPDATE: 'chrome_update',
                            INSTALL: 'install',
                            SHARED_MODULE_UPDATE: 'shared_module_update',
                            UPDATE: 'update'
                        },
                        OnRestartRequiredReason: {
                            APP_UPDATE: 'app_update',
                            OS_UPDATE: 'os_update',
                            PERIODIC: 'periodic'
                        },
                        PlatformArch: {
                            ARM: 'arm',
                            ARM64: 'arm64',
                            MIPS: 'mips',
                            MIPS64: 'mips64',
                            X86_32: 'x86-32',
                            X86_64: 'x86-64'
                        },
                        PlatformNaclArch: {
                            ARM: 'arm',
                            MIPS: 'mips',
                            MIPS64: 'mips64',
                            X86_32: 'x86-32',
                            X86_64: 'x86-64'
                        },
                        PlatformOs: {
                            ANDROID: 'android',
                            CROS: 'cros',
                            LINUX: 'linux',
                            MAC: 'mac',
                            OPENBSD: 'openbsd',
                            WIN: 'win'
                        },
                        RequestUpdateCheckStatus: {
                            NO_UPDATE: 'no_update',
                            THROTTLED: 'throttled',
                            UPDATE_AVAILABLE: 'update_available'
                        }
                    }
                };
                
                // Simuler des fonctionnalités de navigateur
                const originalFunction = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = function(type) {
                    if (type === 'image/png' && this.width === 220 && this.height === 30) {
                        return originalFunction.apply(this, arguments);
                    }
                    return originalFunction.apply(this, arguments);
                };
                
                // Simuler des événements de souris réalistes
                (function() {
                    const oldMoveTo = MouseEvent.prototype.moveTo;
                    MouseEvent.prototype.moveTo = function(...args) {
                        const result = oldMoveTo.apply(this, args);
                        this.screenX = Math.floor(Math.random() * 1920);
                        this.screenY = Math.floor(Math.random() * 1080);
                        return result;
                    };
                })();
            '''
        })
        
        # Configurer le WebGL avec plus de paramètres
        try:
            driver.execute_cdp_cmd('Browser.grantPermissions', {
                'origin': 'https://' + urlparse(driver.current_url).netloc,
                'permissions': [
                    'notifications',
                    'geolocation',
                    'midi',
                    'midiSysex'
                ]
            })
        except Exception as e:
            logging.warning(f"Erreur permissions WebGL: {e}")
        
        # Emuler les caractéristiques du navigateur avec plus de détails
        driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride', {
            'mobile': self.current_profile.mobile,
            'width': self.current_profile.viewport['width'],
            'height': self.current_profile.viewport['height'],
            'deviceScaleFactor': random.uniform(1.0, 2.0),
            'screenWidth': self.current_profile.screen['width'],
            'screenHeight': self.current_profile.screen['height'],
            'positionX': 0,
            'positionY': 0,
            'dontSetVisibleSize': False,
            'screenOrientation': {
                'angle': 0,
                'type': 'portraitPrimary'
            }
        })
        
        # Ajouter des mouvements de souris aléatoires
        driver.execute_cdp_cmd('Input.synthesizeScrollGesture', {
            'x': random.randint(0, self.current_profile.viewport['width']),
            'y': random.randint(0, self.current_profile.viewport['height']),
            'xDistance': random.randint(-100, 100),
            'yDistance': random.randint(-100, 100),
            'repeatCount': random.randint(1, 3),
            'repeatDelayMs': random.randint(100, 500),
            'speed': random.randint(100, 300)
        })
    
    async def get_browser(self) -> webdriver.Chrome:
        """Obtenir un navigateur du pool avec un profil aléatoire"""
        async with self.semaphore:
            # Générer un nouveau profil
            self.current_profile = self.profile_manager.generate_profile()
            
            # Fermer un ancien navigateur si nécessaire
            if len(self.browsers) >= self.max_size:
                old_browser = self.browsers.popleft()
                try:
                    old_browser.quit()
                except Exception:
                    pass
            
            try:
                # Créer un nouveau navigateur avec le profil
                browser = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=self._setup_options(self.current_profile)
                )
                
                # Configurer les commandes CDP
                self._setup_cdp_commands(browser)
                
                # Ajouter au pool
                self.browsers.append(browser)
                
                # Ajouter les cookies au navigateur
                domain = urlparse(browser.current_url).netloc
                if domain:
                    # Charger les cookies existants
                    cookies = self.resource_manager.cookie_manager.load_cookies(domain)
                    if not cookies:
                        # Générer de nouveaux cookies si aucun n'existe
                        cookies = self.resource_manager.cookie_manager.generate_cookies(domain)
                    
                    # Ajouter les cookies au navigateur
                    for cookie in cookies:
                        try:
                            browser.add_cookie(cookie)
                        except Exception as e:
                            logging.warning(f"Erreur ajout cookie: {e}")
                
                return browser
                
            except Exception as e:
                logging.error(f"Erreur création navigateur: {e}")
                raise
            
    async def cleanup(self):
        """Nettoyer tous les navigateurs"""
        while self.browsers:
            browser = self.browsers.popleft()
            try:
                browser.quit()
            except Exception:
                pass

class AffiliateDetector:
    """Détecteur de programmes d'affiliation"""
    def __init__(self):
        self.url_patterns = [
            r'/partners?/?program',
            r'/affiliates?/?program',
            r'/referrals?/?program',
            r'/join-?our-?program',
            r'/become-?(an?-?)?(affiliate|partner|reseller)',
            r'/earn-?with-?us',
            r'/monetize',
            r'/revenue-?share',
            r'/partner-?portal',
            r'/affiliate-?login',
            r'/affiliate-?signup',
            r'/join-?us',
            r'/work-?with-?us',
            r'/partnership',
            r'/reseller',
            r'/ambassador',
            r'/influencer',
            r'/commission',
            r'/rewards?',
            r'/referral-?rewards?',
            r'/affiliate-?marketing',
            r'/partner-?with-?us',
            r'/join-?network',
            r'/partner-?network',
            r'/affiliate-?network',
            r'/partner-?marketplace',
            r'/affiliate-?marketplace',
            r'/partner-?program-?signup',
            r'/affiliate-?program-?signup',
            r'/partner-?registration',
            r'/affiliate-?registration',
            r'/partner-?application',
            r'/affiliate-?application',
            r'/partner-?onboarding',
            r'/affiliate-?onboarding'
        ]
        
        self.content_patterns = [
            r'\$\d+(?:\.\d{2})?\s+per\s+(?:sale|lead|click|referral|signup|conversion)',
            r'\d+%\s+(?:commission|revenue share|profit share|payout)',
            r'(?:earn|make)\s+up\s+to\s+[\$€£]\d+(?:[kK])?',
            r'(?:join|become)\s+(?:our|an?)\s+(?:affiliate|partner|reseller|ambassador|influencer)',
            r'(?:affiliate|partner|referral)\s+program\s+(?:details|benefits|terms|conditions|faq)',
            r'(?:commission|revenue share|payout)\s+(?:structure|rates|details|plan|scheme)',
            r'(?:earn|receive)\s+(?:commission|revenue|income|rewards|bonuses)',
            r'(?:promote|market|sell|recommend)\s+our\s+(?:product|service|platform|solution)',
            r'(?:start|begin)\s+(?:earning|promoting|selling|referring)',
            r'(?:partner|affiliate|reseller)\s+(?:portal|dashboard|login|signup|register)',
            r'(?:join|apply|register)\s+(?:now|today)\s+(?:as|to become)\s+(?:partner|affiliate)',
            r'(?:monthly|recurring)\s+(?:commission|revenue|income)',
            r'(?:two|2|three|3)\s+tier\s+(?:commission|affiliate)',
            r'(?:lifetime|passive)\s+(?:commission|income|earnings)',
            r'(?:high|competitive)\s+(?:commission|payout)\s+rates?',
            r'(?:partner|affiliate)\s+(?:success|support|resources|tools)',
            r'(?:marketing|promotional)\s+(?:materials|resources|tools)',
            r'(?:track|monitor)\s+(?:your|affiliate)\s+(?:sales|performance|earnings)',
            r'(?:real-?time|instant)\s+(?:commission|tracking|reporting)',
            r'(?:exclusive|premium)\s+(?:partner|affiliate)\s+(?:benefits|perks)',
            r'(?:partner|affiliate)\s+(?:agreement|contract|terms)',
            r'(?:apply|register|sign up)\s+(?:for|to)\s+(?:our|the)\s+(?:partner|affiliate)',
            r'(?:partner|affiliate)\s+(?:application|registration|form)',
            r'(?:become|join)\s+(?:a|our)\s+(?:valued|trusted)\s+(?:partner|affiliate)',
            r'(?:partner|affiliate)\s+(?:network|community|ecosystem)',
            r'(?:partnership|collaboration)\s+(?:opportunities|program)',
            r'(?:work|partner|collaborate)\s+with\s+us',
            r'(?:monetize|leverage|grow)\s+(?:your|the)\s+(?:audience|following|community)',
            r'(?:referral|commission)\s+(?:tracking|system|platform)',
            r'(?:partner|affiliate)\s+(?:benefits|advantages|rewards)',
            r'(?:start|grow|boost)\s+(?:your|the)\s+(?:passive|recurring)\s+income',
            r'(?:partner|affiliate)\s+(?:training|resources|support)',
            r'(?:partner|affiliate)\s+(?:faq|help|guide)',
            r'(?:partner|affiliate)\s+(?:terms|conditions|policies)',
            r'(?:partner|affiliate)\s+(?:contact|support|help)',
            r'(?:partner|affiliate)\s+(?:login|portal|dashboard)',
            r'(?:partner|affiliate)\s+(?:signup|registration|onboarding)',
            r'(?:partner|affiliate)\s+(?:marketplace|directory|listing)',
            r'(?:partner|affiliate)\s+(?:tools|resources|assets)',
            r'(?:partner|affiliate)\s+(?:news|updates|blog)',
            r'(?:partner|affiliate)\s+(?:success stories|testimonials|reviews)',
            r'(?:partner|affiliate)\s+(?:events|webinars|training)',
            r'(?:partner|affiliate)\s+(?:tiers|levels|ranks)',
            r'(?:partner|affiliate)\s+(?:requirements|qualifications|criteria)',
            r'(?:partner|affiliate)\s+(?:benefits|perks|rewards)',
            r'(?:partner|affiliate)\s+(?:marketing|promotion|advertising)',
            r'(?:partner|affiliate)\s+(?:resources|materials|creatives)',
            r'(?:partner|affiliate)\s+(?:support|assistance|help)',
            r'(?:partner|affiliate)\s+(?:program|scheme|initiative)',
            r'(?:partner|affiliate)\s+(?:opportunities|possibilities|options)',
            r'(?:partner|affiliate)\s+(?:network|community|ecosystem)',
            r'(?:partner|affiliate)\s+(?:portal|platform|system)',
            r'(?:partner|affiliate)\s+(?:dashboard|analytics|reporting)',
            r'(?:partner|affiliate)\s+(?:tracking|monitoring|measurement)',
            r'(?:partner|affiliate)\s+(?:payments|payouts|commissions)',
            r'(?:partner|affiliate)\s+(?:terms|conditions|agreement)',
            r'(?:partner|affiliate)\s+(?:policy|guidelines|rules)',
            r'(?:partner|affiliate)\s+(?:faq|help|support)',
            r'(?:partner|affiliate)\s+(?:contact|inquiry|question)'
        ]
        
        self.keyword_weights = {
            'affiliate': 5.0,
            'partner': 3.0,
            'referral': 3.0,
            'commission': 4.0,
            'revenue': 3.0,
            'earn': 2.0,
            'promote': 2.0,
            'join': 1.5,
            'become': 1.5,
            'program': 2.0,
            'portal': 1.5,
            'dashboard': 1.5,
            'reseller': 3.0,
            'ambassador': 2.5,
            'influencer': 2.5,
            'monetize': 3.0,
            'rewards': 2.0,
            'payout': 3.0,
            'marketplace': 2.0,
            'network': 2.0,
            'signup': 1.5,
            'register': 1.5,
            'application': 1.5,
            'onboarding': 1.5,
            'partnership': 3.0,
            'collaboration': 2.0,
            'opportunities': 1.5,
            'benefits': 2.0,
            'success': 1.5,
            'support': 1.5,
            'resources': 1.5,
            'training': 1.5,
            'marketing': 2.0,
            'promotional': 2.0,
            'tracking': 2.0,
            'reporting': 1.5,
            'analytics': 1.5,
            'payments': 2.0,
            'terms': 1.5,
            'agreement': 1.5,
            'policy': 1.5,
            'guidelines': 1.5,
            'contact': 1.0,
            'inquiry': 1.0,
            'help': 1.0
        }
        
        self.phrase_weights = {
            'affiliate program': 10.0,
            'partner program': 8.0,
            'referral program': 8.0,
            'earn commission': 9.0,
            'revenue share': 8.0,
            'commission rate': 7.0,
            'become an affiliate': 8.0,
            'join our program': 7.0,
            'promote our': 6.0,
            'partner portal': 7.0,
            'affiliate portal': 7.0,
            'partner dashboard': 7.0,
            'affiliate dashboard': 7.0,
            'partner network': 7.0,
            'affiliate network': 7.0,
            'partner marketplace': 7.0,
            'affiliate marketplace': 7.0,
            'partner signup': 6.0,
            'affiliate signup': 6.0,
            'partner registration': 6.0,
            'affiliate registration': 6.0,
            'partner application': 6.0,
            'affiliate application': 6.0,
            'partner onboarding': 6.0,
            'affiliate onboarding': 6.0,
            'partner benefits': 6.0,
            'affiliate benefits': 6.0,
            'partner success': 6.0,
            'affiliate success': 6.0,
            'partner support': 5.0,
            'affiliate support': 5.0,
            'partner resources': 5.0,
            'affiliate resources': 5.0,
            'partner training': 5.0,
            'affiliate training': 5.0,
            'partner marketing': 6.0,
            'affiliate marketing': 6.0,
            'partner promotional': 6.0,
            'affiliate promotional': 6.0,
            'partner tracking': 6.0,
            'affiliate tracking': 6.0,
            'partner reporting': 5.0,
            'affiliate reporting': 5.0,
            'partner analytics': 5.0,
            'affiliate analytics': 5.0,
            'partner payments': 6.0,
            'affiliate payments': 6.0,
            'partner terms': 5.0,
            'affiliate terms': 5.0,
            'partner agreement': 5.0,
            'affiliate agreement': 5.0,
            'partner policy': 5.0,
            'affiliate policy': 5.0,
            'partner guidelines': 5.0,
            'affiliate guidelines': 5.0,
            'partner contact': 4.0,
            'affiliate contact': 4.0,
            'partner inquiry': 4.0,
            'affiliate inquiry': 4.0,
            'partner help': 4.0,
            'affiliate help': 4.0,
            'monthly commission': 7.0,
            'recurring commission': 7.0,
            'lifetime commission': 7.0,
            'passive income': 6.0,
            'high commission': 6.0,
            'competitive commission': 6.0,
            'real-time tracking': 6.0,
            'instant commission': 6.0,
            'exclusive benefits': 6.0,
            'premium benefits': 6.0,
            'valued partner': 5.0,
            'trusted partner': 5.0,
            'monetize your': 6.0,
            'grow your': 5.0,
            'boost your': 5.0,
            'track your': 5.0,
            'monitor your': 5.0,
            'your earnings': 6.0,
            'your commission': 6.0,
            'your performance': 5.0,
            'your success': 5.0,
            'work with us': 5.0,
            'partner with us': 6.0,
            'collaborate with us': 5.0,
            'join us': 4.0,
            'join today': 4.0,
            'apply now': 4.0,
            'register now': 4.0,
            'sign up now': 4.0,
            'get started': 4.0
        }
    
    def analyze_url(self, url: str) -> Tuple[float, List[str]]:
        """Analyser une URL pour détecter les indicateurs d'affiliation"""
        url_lower = url.lower()
        score = 0.0
        keywords_found = []
        
        # Vérifier les patterns d'URL
        for pattern in self.url_patterns:
            match = re.search(pattern, url_lower)
            if match:
                score += 5.0
                keywords_found.append(match.group(0))
                break
        
        # Vérifier les mots-clés
        for keyword, weight in self.keyword_weights.items():
            if keyword in url_lower:
                score += weight * 0.5
                keywords_found.append(keyword)
        
        # Vérifier les phrases
        for phrase, weight in self.phrase_weights.items():
            if phrase in url_lower:
                score += weight * 0.3
                keywords_found.append(phrase)
        
        # Bonus pour les chemins d'URL spécifiques
        path = urlparse(url_lower).path
        if '/affiliate' in path or '/partner' in path:
            score += 3.0
        if '/program' in path or '/join' in path:
            score += 2.0
        
        return min(score / 15.0, 1.0), list(set(keywords_found))
    
    def analyze_content(self, text: str) -> Tuple[float, List[str]]:
        """Analyser le contenu pour détecter les indicateurs d'affiliation"""
        if not text:
            return 0.0, []
        
        text_lower = text.lower()
        score = 0.0
        keywords_found = []
        
        # Vérifier les patterns de contenu
        for pattern in self.content_patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                score += 3.0
                keywords_found.append(match.group(0))
        
        # Vérifier les mots-clés
        for keyword, weight in self.keyword_weights.items():
            count = text_lower.count(keyword)
            if count > 0:
                # Bonus pour les occurrences multiples, mais avec diminution
                score += weight * min(count, 3) * 0.8
                keywords_found.append(keyword)
        
        # Vérifier les phrases
        for phrase, weight in self.phrase_weights.items():
            count = text_lower.count(phrase)
            if count > 0:
                # Bonus pour les occurrences multiples de phrases importantes
                score += weight * min(count, 2)
                keywords_found.append(phrase)
        
        # Bonus pour la densité de mots-clés
        total_words = len(text_lower.split())
        if total_words > 0:
            keyword_density = len(keywords_found) / total_words
            if keyword_density > 0.01:  # Plus de 1% de mots-clés
                score *= 1.2
            if keyword_density > 0.05:  # Plus de 5% de mots-clés
                score *= 1.3
        
        # Bonus pour la proximité des mots-clés
        words = text_lower.split()
        for i in range(len(words) - 1):
            if words[i] in self.keyword_weights and words[i + 1] in self.keyword_weights:
                score += (self.keyword_weights[words[i]] + self.keyword_weights[words[i + 1]]) * 0.2
        
        # Bonus pour les sections spécifiques
        if 'program details' in text_lower or 'program benefits' in text_lower:
            score *= 1.2
        if 'commission rates' in text_lower or 'payout rates' in text_lower:
            score *= 1.2
        if 'sign up now' in text_lower or 'join now' in text_lower:
            score *= 1.1
        
        # Pénalité pour les textes trop courts
        if total_words < 50:
            score *= 0.8
        
        return min(score / 25.0, 1.0), list(set(keywords_found))

class AffiliateCrawler:
    """Crawler principal avec approche hybride"""
    def __init__(
        self,
        max_pages: int = 10,
        batch_size: int = 5000,
        headless: bool = True,
        max_concurrent: int = 3,
        memory_limit: int = 1024,
        base_timeout: int = 15000,
        requests_timeout: int = 10,
        max_retries: int = 2,
        min_confidence: float = 0.7
    ):
        self.max_pages = max_pages
        self.batch_size = batch_size
        self.headless = headless
        self.max_concurrent = max_concurrent
        self.base_timeout = base_timeout
        self.requests_timeout = requests_timeout
        self.max_retries = max_retries
        self.min_confidence = min_confidence
        
        # Gestionnaires
        self.resource_manager = ResourceManager(memory_limit)
        self.detector = AffiliateDetector()
        self.browser_pool = BrowserPool(max_concurrent, headless)
        
        # Fichiers
        self.results_file = "affiliate_results_v2.csv"
        self.progress_file = "affiliate_progress_v2.json"
        
        # Headers HTTP
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/96.0.4664.110',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        }
        
        # Session requests
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.verify = False
        
        # Initialisation
        self._init_files()
        self.progress = self._load_progress()
        
        # Nettoyage à la fermeture
        atexit.register(lambda: asyncio.run(self.browser_pool.cleanup()))
        
        # Regex pour les emails
        self.email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extraire les emails d'un texte"""
        if not text:
            return []
        return list(set(self.email_regex.findall(text)))
    
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
                    'score_confiance',
                    'date_analyse'
                ])
    
    def _load_progress(self) -> Dict:
        """Charger la progression"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Erreur chargement progression: {e}")
        
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
        """Sauvegarder la progression"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logging.error(f"Erreur sauvegarde progression: {e}")
    
    def _save_result(self, result: CrawlResult):
        """Sauvegarder un résultat"""
        try:
            with open(self.results_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    result.tool_name,
                    result.url_root,
                    result.status_code,
                    'oui' if result.affiliate_found else 'non',
                    result.affiliate_url or result.url_root,
                    '; '.join(result.emails[:3]) if result.emails else '',
                    result.pages_checked,
                    ', '.join(result.keywords_found),
                    result.method_used,
                    f"{result.confidence_score:.2f}",
                    datetime.now().isoformat()
                ])
        except Exception as e:
            logging.error(f"Erreur sauvegarde résultat: {e}")
    
    def _validate_url(self, url: str) -> Optional[str]:
        """Valider et normaliser une URL"""
        if not url or not isinstance(url, str) or url == 'nan':
            return None
        
        url = url.strip()
        if not url:
            return None
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return None
            return url
        except Exception:
            return None
    

    
    async def crawl_with_requests(self, tool_name: str, url: str) -> Optional[CrawlResult]:
        """Crawler avec requests"""
        try:
            url = self._validate_url(url)
            if not url:
                return None
            
            if not self.resource_manager.check_memory():
                logging.warning(f"Limite mémoire: {self.resource_manager.get_memory_usage():.1f}MB")
                return None
            
            result = CrawlResult(
                tool_name=tool_name,
                url_root=url,
                status_code='ERROR',
                affiliate_found=False,
                affiliate_url='',
                emails=[],
                keywords_found=[],
                pages_checked=0,
                method_used='requests',
                confidence_score=0.0
            )
            
            # Fonction pour faire une requête avec retry et backoff exponentiel
            async def make_request(url: str, timeout: int = None) -> Optional[requests.Response]:
                for attempt in range(self.max_retries):
                    try:
                        current_timeout = timeout or (self.requests_timeout * (2 ** attempt))
                        response = self.session.get(
                            url,
                            timeout=current_timeout,
                            allow_redirects=True,
                            headers=self.headers
                        )
                        response.raise_for_status()
                        return response
                    except requests.Timeout:
                        if attempt == self.max_retries - 1:
                            logging.warning(f"Timeout final pour {url}")
                            return None
                        await asyncio.sleep(2 ** attempt)
                    except requests.RequestException as e:
                        if attempt == self.max_retries - 1:
                            logging.warning(f"Erreur finale pour {url}: {e}")
                            return None
                        if isinstance(e, requests.HTTPError) and e.response.status_code in [429, 503]:
                            await asyncio.sleep(5 * (2 ** attempt))
                        else:
                            await asyncio.sleep(2 ** attempt)
                return None
            
            # Récupérer la page d'accueil
            response = await make_request(url)
            if not response:
                result.status_code = 'ERROR'
                return result
            
            result.status_code = str(response.status_code)
            result.pages_checked += 1
            
            # Analyser la page d'accueil
            soup = BeautifulSoup(response.text, 'html.parser')
            result.emails = self._extract_emails(response.text)
            
            url_score, url_keywords = self.detector.analyze_url(url)
            content_score, content_keywords = self.detector.analyze_content(response.text)
            result.confidence_score = max(url_score, content_score)
            result.keywords_found.extend(url_keywords + content_keywords)
            
            if result.confidence_score >= self.min_confidence:
                result.affiliate_found = True
                result.affiliate_url = url
                return result
            
            # Trouver les liens internes
            internal_links = set()
            for a in soup.find_all('a', href=True):
                try:
                    link = urljoin(url, a['href'])
                    if urlparse(link).netloc == urlparse(url).netloc:
                        internal_links.add(link)
                except Exception:
                    continue
            
            # Filtrer et trier les liens par pertinence
            scored_links = []
            for link in internal_links:
                url_score, _ = self.detector.analyze_url(link)
                if url_score > 0.3:  # Seuil minimal pour explorer
                    scored_links.append((url_score, link))
            
            # Prendre les meilleurs liens dans la limite max_pages
            scored_links.sort(reverse=True)
            best_links = [link for _, link in scored_links[:self.max_pages-1]]
            
            # Explorer les meilleurs liens
            for link in best_links:
                try:
                    response = await make_request(link, timeout=self.requests_timeout)
                    if not response:
                        continue
                    
                    result.pages_checked += 1
                    result.emails.extend(self._extract_emails(response.text))
                    
                    url_score, url_keywords = self.detector.analyze_url(link)
                    content_score, content_keywords = self.detector.analyze_content(response.text)
                    page_score = max(url_score, content_score)
                    result.keywords_found.extend(url_keywords + content_keywords)
                    
                    if page_score > result.confidence_score:
                        result.confidence_score = page_score
                        if page_score >= self.min_confidence:
                            result.affiliate_found = True
                            result.affiliate_url = link
                            break
                    
                    # Pause courte entre les requêtes
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logging.warning(f"Erreur exploration {link}: {e}")
            
            # Nettoyer et dédupliquer les résultats
            result.emails = list(set(result.emails))
            result.keywords_found = list(set(result.keywords_found))
            return result
            
        except Exception as e:
            logging.error(f"Erreur requests pour {tool_name}: {e}")
            return None
    
    async def crawl_with_selenium(self, tool_name: str, url: str) -> Optional[CrawlResult]:
        """Crawler avec Selenium"""
        browser = None
        try:
            url = self._validate_url(url)
            if not url:
                return None
            
            if not self.resource_manager.check_memory():
                logging.warning(f"Limite mémoire: {self.resource_manager.get_memory_usage():.1f}MB")
                return None
            
            result = CrawlResult(
                tool_name=tool_name,
                url_root=url,
                status_code='ERROR',
                affiliate_found=False,
                affiliate_url='',
                emails=[],
                keywords_found=[],
                pages_checked=0,
                method_used='selenium',
                confidence_score=0.0
            )
            
            browser = await self.browser_pool.get_browser()
            wait = WebDriverWait(browser, self.requests_timeout)
            
            # Fonction pour naviguer avec retry et backoff exponentiel
            async def navigate_to_url(url: str, timeout: int = None) -> Optional[bool]:
                for attempt in range(self.max_retries):
                    try:
                        current_timeout = timeout or (self.requests_timeout * (2 ** attempt))
                        browser.set_page_load_timeout(current_timeout)
                        
                        # Ajouter des headers aléatoires
                        browser.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
                            'headers': {
                                'Accept-Language': random.choice([
                                    'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                                    'en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7',
                                    'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7'
                                ]),
                                'Referer': 'https://www.google.com/',
                                'DNT': '1',
                                'Upgrade-Insecure-Requests': '1',
                                'Cache-Control': random.choice([
                                    'max-age=0',
                                    'no-cache',
                                    'no-store'
                                ])
                            }
                        })
                        
                        # Simuler un comportement humain avant la navigation
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        
                        browser.get(url)
                        
                        # Attendre que le body soit présent
                        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                        
                        # Simuler un scroll progressif
                        total_height = browser.execute_script('return Math.max(document.documentElement.scrollHeight, document.body.scrollHeight);')
                        current_height = 0
                        scroll_step = random.randint(200, 400)
                        
                        while current_height < total_height:
                            next_height = min(current_height + scroll_step, total_height)
                            browser.execute_script(f'window.scrollTo({current_height}, {next_height});')
                            current_height = next_height
                            await asyncio.sleep(random.uniform(0.1, 0.3))
                        
                        # Simuler des mouvements de souris réalistes
                        actions = webdriver.ActionChains(browser)
                        elements = browser.find_elements(By.TAG_NAME, 'a')[:5]
                        
                        # Position initiale aléatoire
                        start_x = random.randint(0, browser.get_window_size()['width'])
                        start_y = random.randint(0, browser.get_window_size()['height'])
                        actions.move_by_offset(start_x, start_y).perform()
                        
                        for element in elements:
                            try:
                                # Mouvement progressif vers l'élément
                                rect = element.rect
                                target_x = rect['x'] + rect['width'] / 2
                                target_y = rect['y'] + rect['height'] / 2
                                
                                # Diviser le mouvement en plusieurs étapes
                                steps = random.randint(5, 10)
                                current_x = start_x
                                current_y = start_y
                                
                                for step in range(steps):
                                    next_x = current_x + (target_x - current_x) / (steps - step)
                                    next_y = current_y + (target_y - current_y) / (steps - step)
                                    
                                    # Ajouter une légère déviation aléatoire
                                    next_x += random.uniform(-5, 5)
                                    next_y += random.uniform(-5, 5)
                                    
                                    actions.move_by_offset(next_x - current_x, next_y - current_y).perform()
                                    await asyncio.sleep(random.uniform(0.05, 0.1))
                                    
                                    current_x = next_x
                                    current_y = next_y
                                
                                # Pause sur l'élément
                                await asyncio.sleep(random.uniform(0.2, 0.5))
                                
                                # 20% de chance de cliquer
                                if random.random() < 0.2:
                                    element.click()
                                    await asyncio.sleep(random.uniform(0.5, 1.0))
                                    browser.back()
                                    await asyncio.sleep(random.uniform(0.3, 0.7))
                                
                                start_x = current_x
                                start_y = current_y
                                
                            except Exception:
                                pass
                        
                        # Simuler le code de statut
                        result.status_code = '200'
                        return True
                        
                    except TimeoutException:
                        if attempt == self.max_retries - 1:
                            logging.warning(f"Timeout final pour {url}")
                            return False
                        await asyncio.sleep(2 ** attempt)
                    except WebDriverException as e:
                        if attempt == self.max_retries - 1:
                            logging.warning(f"Erreur finale pour {url}: {e}")
                            return False
                        await asyncio.sleep(2 ** attempt)
                return False
            
            # Naviguer vers la page d'accueil
            if not await navigate_to_url(url):
                return result
            
            result.pages_checked += 1
            
            # Analyser la page d'accueil
            text = browser.find_element(By.TAG_NAME, 'body').text
            
            result.emails = self._extract_emails(text)
            url_score, url_keywords = self.detector.analyze_url(url)
            content_score, content_keywords = self.detector.analyze_content(text)
            result.confidence_score = max(url_score, content_score)
            result.keywords_found.extend(url_keywords + content_keywords)
            
            if result.confidence_score >= self.min_confidence:
                result.affiliate_found = True
                result.affiliate_url = url
                return result
            
            # Trouver et scorer les liens internes
            links = browser.find_elements(By.TAG_NAME, 'a')
            scored_links = []
            
            for link in links:
                try:
                    href = link.get_attribute('href')
                    if not href:
                        continue
                    
                    full_url = urljoin(url, href)
                    if urlparse(full_url).netloc != urlparse(url).netloc:
                        continue
                    
                    # Scorer le lien
                    link_text = link.text
                    url_score, _ = self.detector.analyze_url(full_url)
                    text_score, _ = self.detector.analyze_content(link_text)
                    link_score = max(url_score, text_score)
                    
                    if link_score > 0.3:  # Seuil minimal pour explorer
                        scored_links.append((link_score, full_url))
                except Exception:
                    continue
            
            # Explorer les meilleurs liens
            scored_links.sort(reverse=True)
            best_links = [link for _, link in scored_links[:self.max_pages-1]]
            
            for link in best_links:
                try:
                    if not await navigate_to_url(link, timeout=self.requests_timeout):
                        continue
                    
                    result.pages_checked += 1
                    
                    text = browser.find_element(By.TAG_NAME, 'body').text
                    result.emails.extend(self._extract_emails(text))
                    
                    url_score, url_keywords = self.detector.analyze_url(link)
                    content_score, content_keywords = self.detector.analyze_content(text)
                    page_score = max(url_score, content_score)
                    result.keywords_found.extend(url_keywords + content_keywords)
                    
                    if page_score > result.confidence_score:
                        result.confidence_score = page_score
                        if page_score >= self.min_confidence:
                            result.affiliate_found = True
                            result.affiliate_url = link
                            break
                    
                    # Pause courte entre les pages
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logging.warning(f"Erreur exploration {link}: {e}")
            
            # Nettoyer et dédupliquer les résultats
            result.emails = list(set(result.emails))
            result.keywords_found = list(set(result.keywords_found))
            return result
            
        except Exception as e:
            logging.error(f"Erreur critique Selenium pour {tool_name}: {e}")
            return result
            
        finally:
            if browser:
                try:
                    browser.quit()
                except Exception:
                    pass
    
    async def process_tool(self, tool_name: str, url: str) -> None:
        """Traiter un outil"""
        try:
            print(f"\nTraitement de: {tool_name} ({url})")
            
            if tool_name in self.progress['processed_tools']:
                print(f"✓ {tool_name} déjà traité")
                return
            
            if not url or not isinstance(url, str) or url == 'nan':
                print(f"❌ {tool_name}: URL invalide")
                self.progress['stats']['ERROR'] += 1
                self.progress['processed_tools'].append(tool_name)
                self._save_progress()
                return
            
            # Vérifier les ressources système
            memory_usage = self.resource_manager.get_memory_usage()
            cpu_usage = self.resource_manager.get_cpu_usage()
            
            if memory_usage > self.resource_manager.memory_limit * 0.9:
                print(f"⚠️ Mémoire haute: {memory_usage:.1f}MB")
                await asyncio.sleep(5)
            
            if cpu_usage > 80:
                print(f"⚠️ CPU élevé: {cpu_usage:.1f}%")
                await asyncio.sleep(5)
            
            # Essayer d'abord avec requests
            result = await self.crawl_with_requests(tool_name, url)
            
            # Si requests échoue ou ne trouve pas d'affiliation avec un score suffisant
            if not result or (not result.affiliate_found and result.confidence_score < self.min_confidence):
                print(f"⚡ {tool_name}: Tentative avec Selenium...")
                try:
                    # Pause avant de passer à Selenium
                    await asyncio.sleep(1)
                    result = await self.crawl_with_selenium(tool_name, url)
                except Exception as e:
                    logging.error(f"Erreur Selenium pour {tool_name}: {e}")
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
                
                # Sauvegarder la progression
                self._save_progress()
                
                # Afficher le résultat
                status = '✅' if result.affiliate_found else '❌'
                print(f"{status} {tool_name}:")
                print(f"  - Affiliation: {result.affiliate_found}")
                print(f"  - Méthode: {result.method_used}")
                print(f"  - Score: {result.confidence_score:.2f}")
                print(f"  - Pages: {result.pages_checked}")
                if result.affiliate_found:
                    print(f"  - URL: {result.affiliate_url}")
                if result.emails:
                    print(f"  - Emails: {', '.join(result.emails[:3])}")
                if result.keywords_found:
                    print(f"  - Mots-clés: {', '.join(result.keywords_found[:5])}")
            else:
                print(f"❌ {tool_name}: Erreur")
                self.progress['stats']['ERROR'] += 1
                self.progress['processed_tools'].append(tool_name)
                self._save_progress()
            
            # Pause entre les outils
            await asyncio.sleep(1)
                
        except Exception as e:
            logging.error(f"Erreur traitement {tool_name}: {e}")
            print(f"❌ {tool_name}: Erreur: {str(e)[:100]}")
            self.progress['stats']['ERROR'] += 1
            self.progress['processed_tools'].append(tool_name)
            self._save_progress()
            
            # Pause plus longue en cas d'erreur
            await asyncio.sleep(2)
    
    async def run(self, tools_data: pd.DataFrame, is_test: bool = False):
        """Exécuter le crawler"""
        try:
            # Configurer le gestionnaire de signaux pour une fermeture propre
            def cleanup():
                print("\n⚠️ Signal d'arrêt reçu. Nettoyage en cours...")
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(self.browser_pool.cleanup())
                    else:
                        loop.run_until_complete(self.browser_pool.cleanup())
                except Exception as e:
                    logging.error(f"Erreur nettoyage: {e}")
                print("👋 Au revoir !")
            
            def signal_handler(signum, frame):
                cleanup()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            atexit.register(cleanup)
            
            # Préparer les outils à traiter
            if is_test:
                tools_to_process = tools_data.head(10)
                print(f"Mode TEST: {len(tools_to_process)} outils")
            else:
                tools_to_process = tools_data
                print(f"Mode PRODUCTION: {len(tools_to_process)} outils")
            
            # Filtrer les outils déjà traités
            tools_list = []
            for _, row in tools_to_process.iterrows():
                tool_name = row['tool_name']
                tool_link = row['tool_link']
                
                if tool_name in self.progress['processed_tools']:
                    continue
                
                # Vérifier et nettoyer l'URL
                if pd.isna(tool_link) or not isinstance(tool_link, str):
                    print(f"⚠️ URL invalide pour {tool_name}")
                    continue
                
                tool_link = tool_link.strip()
                if not tool_link:
                    print(f"⚠️ URL vide pour {tool_name}")
                    continue
                
                tools_list.append((tool_name, tool_link))
            
            if not tools_list:
                print("Aucun nouvel outil à traiter")
                return
            
            print(f"Outils à traiter: {len(tools_list)}")
            
            # Calculer les batches
            batch_size = min(self.batch_size, len(tools_list))
            total_batches = (len(tools_list) + batch_size - 1) // batch_size
            
            # Traiter les batches
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(tools_list))
                batch = tools_list[start_idx:end_idx]
                
                print(f"\n{'='*80}")
                print(f"BATCH {batch_num + 1}/{total_batches}")
                print(f"{'='*80}")
                
                # Créer un sémaphore pour limiter la concurrence
                semaphore = asyncio.Semaphore(self.max_concurrent)
                
                async def process_with_semaphore(tool):
                    async with semaphore:
                        await self.process_tool(*tool)
                
                # Exécuter les tâches en parallèle
                tasks = [process_with_semaphore(tool) for tool in batch]
                try:
                    await asyncio.gather(*tasks)
                except Exception as e:
                    logging.error(f"Erreur dans le batch {batch_num + 1}: {e}")
                
                # Afficher les statistiques du batch
                print(f"\nBatch {batch_num + 1} terminé:")
                print(f"  Outils traités: {len(self.progress['processed_tools'])}")
                print(f"  Affiliations trouvées: {self.progress['stats']['AFFILIATE_FOUND']}")
                print(f"  Requests utilisé: {self.progress['stats']['REQUESTS_USED']}")
                print(f"  Playwright utilisé: {self.progress['stats']['PLAYWRIGHT_USED']}")
                print(f"  Erreurs: {self.progress['stats']['ERROR']}")
                print(f"  Mémoire: {self.resource_manager.get_memory_usage():.1f}MB")
                print(f"  CPU: {self.resource_manager.get_cpu_usage():.1f}%")
                
                # Pause entre les batches
                if batch_num < total_batches - 1:
                    print("\nPause de 3 secondes...")
                    await asyncio.sleep(3)
                    
                    # Vérifier les ressources
                    memory_usage = self.resource_manager.get_memory_usage()
                    cpu_usage = self.resource_manager.get_cpu_usage()
                    
                    if memory_usage > self.resource_manager.memory_limit * 0.9:
                        print(f"⚠️ Mémoire haute ({memory_usage:.1f}MB). Pause prolongée...")
                        await asyncio.sleep(10)
                    
                    if cpu_usage > 80:
                        print(f"⚠️ CPU élevé ({cpu_usage:.1f}%). Pause prolongée...")
                        await asyncio.sleep(10)
            
            # Afficher le résumé final
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
            
            # Calculer les taux de réussite
            total_processed = len(self.progress['processed_tools'])
            if total_processed > 0:
                success_rate = (self.progress['stats']['OK'] / total_processed) * 100
                affiliate_rate = (self.progress['stats']['AFFILIATE_FOUND'] / total_processed) * 100
                requests_rate = (self.progress['stats']['REQUESTS_USED'] / total_processed) * 100
                print(f"\n📈 STATISTIQUES:")
                print(f"  • Taux de succès: {success_rate:.1f}%")
                print(f"  • Taux d'affiliation: {affiliate_rate:.1f}%")
                print(f"  • Taux requests vs playwright: {requests_rate:.1f}%")
            
        except Exception as e:
            logging.error(f"Erreur générale: {e}")
            print(f"❌ Erreur générale: {e}")
            
        finally:
            # Fermer proprement les navigateurs
            try:
                await self.browser_pool.cleanup()
            except Exception as e:
                logging.error(f"Erreur fermeture navigateurs: {e}")
            
            print("\n👋 Au revoir !")

async def main():
    parser = argparse.ArgumentParser(
        description='Crawler hybride optimisé pour détecter les programmes d\'affiliation'
    )
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Taille des batches (défaut: 5000)')
    parser.add_argument('--max-pages', type=int, default=10,
                       help='Pages max par site (défaut: 10)')
    parser.add_argument('--max-concurrent', type=int, default=3,
                       help='Pages simultanées max (défaut: 3)')
    parser.add_argument('--memory-limit', type=int, default=1024,
                       help='Limite mémoire en MB (défaut: 1024)')
    parser.add_argument('--base-timeout', type=int, default=15000,
                       help='Timeout de base en ms (défaut: 15000)')
    parser.add_argument('--requests-timeout', type=int, default=10,
                       help='Timeout requests en secondes (défaut: 10)')
    parser.add_argument('--max-retries', type=int, default=2,
                       help='Nombre max de tentatives (défaut: 2)')
    parser.add_argument('--min-confidence', type=float, default=0.7,
                       help='Score minimum de confiance (défaut: 0.7)')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Mode headless (défaut: True)')
    parser.add_argument('--test', action='store_true',
                       help='Mode test (10 premiers outils)')
    parser.add_argument('--debug', action='store_true',
                       help='Mode debug avec plus de logs')
    parser.add_argument('--input-file', type=str, default='tools.csv',
                       help='Fichier CSV d\'entrée (défaut: tools.csv)')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Vérifier le fichier d'entrée
        if not os.path.exists(args.input_file):
            print(f"❌ Fichier {args.input_file} introuvable")
            return
        
        # Charger les données
        try:
            df = pd.read_csv(args.input_file)
            required_columns = ['tool_name', 'tool_link']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"❌ Colonnes manquantes: {', '.join(missing_columns)}")
                return
        except Exception as e:
            print(f"❌ Erreur lecture CSV: {e}")
            return
        
        # Créer le crawler
        crawler = AffiliateCrawler(
            max_pages=args.max_pages,
            batch_size=args.batch_size,
            headless=args.headless,
            max_concurrent=args.max_concurrent,
            memory_limit=args.memory_limit,
            base_timeout=args.base_timeout,
            requests_timeout=args.requests_timeout,
            max_retries=args.max_retries,
            min_confidence=args.min_confidence
        )
        
        # Exécuter le crawler
        print("\n🚀 Démarrage du crawler...")
        print(f"📋 Configuration:")
        print(f"  • Batch size: {args.batch_size}")
        print(f"  • Max pages: {args.max_pages}")
        print(f"  • Max concurrent: {args.max_concurrent}")
        print(f"  • Memory limit: {args.memory_limit}MB")
        print(f"  • Base timeout: {args.base_timeout}ms")
        print(f"  • Requests timeout: {args.requests_timeout}s")
        print(f"  • Max retries: {args.max_retries}")
        print(f"  • Min confidence: {args.min_confidence}")
        print(f"  • Mode: {'TEST' if args.test else 'PRODUCTION'}")
        print(f"  • Debug: {'OUI' if args.debug else 'NON'}")
        
        await crawler.run(df, is_test=args.test)
        
    except KeyboardInterrupt:
        print("\n⚠️ Interruption utilisateur")
    except Exception as e:
        logging.error(f"Erreur générale: {e}")
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Interruption utilisateur")
    except Exception as e:
        print(f"❌ Erreur fatale: {e}")
    finally:
        print("\n👋 Au revoir !")
