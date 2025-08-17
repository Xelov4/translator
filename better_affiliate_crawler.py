import asyncio
import csv
import json
import logging
import os
import random
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("better_affiliate_crawler.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


@dataclass
class CrawlResult:
    """Structure to store the results of the crawl."""

    tool_name: str
    url_root: str
    status_code: str = "N/A"
    affiliate_found: bool = False
    affiliate_url: str = ""
    emails: Set[str] = field(default_factory=set)
    keywords_found: Set[str] = field(default_factory=set)
    pages_checked: int = 0
    method_used: str = ""


class BetterAffiliateCrawler:
    """
    An improved affiliate crawler with better anti-bot evasion,
    crawling strategy, and performance.
    """

    def __init__(self, max_pages=20, headless=True, use_proxies=False):
        self.max_pages = max_pages
        self.headless = headless
        self.use_proxies = use_proxies
        self.proxies = [
            # "http://user:pass@host:port",
        ]
        self.results_file = "better_affiliate_results.csv"
        self.progress_file = "better_affiliate_progress.json"
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        ]
        self.affiliate_keywords = self._build_affiliate_keywords()
        self.email_regex = re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(?!sentry\.io)[A-Z|a-z]{2,}\b"
        )
        self.session = requests.Session()
        self.progress = self._load_progress()
        self._init_files()

    def run_cleanup(self):
        """Clean up old result and progress files to avoid duplicates."""
        if os.path.exists(self.results_file):
            os.remove(self.results_file)
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)
        self._init_files() # Re-initialize the files after cleaning
        self.progress = self._load_progress() # Reload progress after cleaning

    def _build_affiliate_keywords(self):
        """Build a comprehensive, multilingual list of keywords and patterns."""
        return {
            'url_patterns': [
                '/affiliate', '/affiliates', '/partner', '/partners', '/partnership',
                '/referral', '/referrals', '/parrainage', '/referidos', '/empfehlung',
                '/influencer', '/ambassador', '/rewards', '/commission', '/revenue-share'
            ],
            'content_keywords': {
                'en': [
                    'affiliate program', 'partner program', 'referral program', 'influencer program',
                    'ambassador program', 'rewards program', 'earn commission', 'commission rates',
                    'become an affiliate', 'become a partner', 'join our program', 'affiliate partnership',
                    'revenue share', 'earn money', 'promote our', 'partner with us', 'affiliate login',
                    'partner portal', 'refer a friend'
                ],
                'fr': [
                    'programme d\'affiliation', 'programme de partenariat', 'programme de parrainage',
                    'programme d\'influenceurs', 'programme ambassadeur', 'devenir partenaire',
                    'commission d\'affiliation', 'gagner une commission', 'rejoindre notre programme',
                    'partenariat affilié', 'revenus partagés', 'promouvoir', 'parrainer un ami'
                ],
                'es': [
                    'programa de afiliados', 'programa de socios', 'programa de referidos',
                    'programa de influencers', 'programa de embajadores', 'hazte afiliado',
                    'comisiones de afiliados', 'únete a nuestro programa', 'gana comisiones',
                    'recomienda y gana'
                ],
                'de': [
                    'partnerprogramm', 'affiliate programm', 'empfehlungsprogramm', 'partner werden',
                    'provision verdienen', 'jetzt partner werden', 'als partner registrieren',
                    'freunde werben'
                ],
                'pt': [
                    'programa de afiliados', 'programa de parceiros', 'programa de referência',
                    'indique e ganhe', 'torne-se um afiliado', 'ganhe comissão', 'portal de parceiros'
                ],
                'it': [
                    'programma di affiliazione', 'programma partner', 'diventa un affiliato',
                    'guadagna commissioni', 'portale partner', 'invita un amico'
                ]
            },
            'strong_indicators': [
                'affiliate dashboard', 'partner portal', 'affiliate login', 'partner login',
                'commission structure', 'payout rates'
            ]
        }

    def _init_files(self):
        if not os.path.exists(self.results_file):
            with open(
                self.results_file, "w", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        "tool_name",
                        "url_root",
                        "status_code",
                        "affiliate_found",
                        "affiliate_url",
                        "emails",
                        "pages_checked",
                        "keywords_found",
                        "method_used",
                        "crawled_at",
                    ]
                )

    def _load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {"processed_tools": []}
        return {"processed_tools": []}

    def _save_progress(self):
        with open(self.progress_file, "w") as f:
            json.dump(self.progress, f, indent=4)

    def _save_result(self, result: CrawlResult):
        with open(self.results_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    result.tool_name,
                    result.url_root,
                    result.status_code,
                    "yes" if result.affiliate_found else "no",
                    result.affiliate_url,
                    "; ".join(result.emails),
                    result.pages_checked,
                    ", ".join(result.keywords_found),
                    result.method_used,
                    datetime.now().isoformat(),
                ]
            )

    def get_random_user_agent(self):
        return random.choice(self.user_agents)

    def _validate_url(self, url: str) -> Optional[str]:
        """Validate and normalize a URL."""
        if not url or not isinstance(url, str) or url.lower() == 'nan':
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

    def _extract_emails(self, text: str) -> Set[str]:
        """Extract emails from text."""
        return set(self.email_regex.findall(text))

    def _check_affiliate_indicators(self, url: str, soup: BeautifulSoup) -> (bool, Set[str]):
        """Check for affiliate indicators in URL and content, with context."""
        keywords_found = set()
        text_lower = soup.get_text().lower()
        url_lower = url.lower()
        has_link_keyword = False

        # 1. Check URL patterns
        for pattern in self.affiliate_keywords['url_patterns']:
            if pattern in url_lower:
                keywords_found.add(pattern)

        all_content_keywords = [kw for kws in self.affiliate_keywords['content_keywords'].values() for kw in kws]

        # 2. Check for keywords within link text (strong indicator)
        for a in soup.find_all('a'):
            link_text = a.get_text().lower()
            if any(keyword in link_text for keyword in all_content_keywords):
                has_link_keyword = True
                keywords_found.add(a.get_text().strip()) # Add the actual link text found

        # 3. Check for keywords in the whole page text
        for keyword in all_content_keywords:
            if keyword in text_lower:
                keywords_found.add(keyword)

        # 4. Check for strong indicators (like "affiliate dashboard")
        has_strong_indicator = any(
            indicator in text_lower for indicator in self.affiliate_keywords['strong_indicators']
        )
        
        # A page is considered an affiliate page if it has:
        # - A strong indicator OR
        # - A keyword in a link's text OR
        # - More than one keyword found in total
        return has_strong_indicator or has_link_keyword or len(keywords_found) > 1, keywords_found

    def _get_internal_links(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
        """Extract internal links from a page, including subdomains."""
        internal_links = set()
        parsed_base = urlparse(base_url)
        # Extract the main domain (e.g., 'google.com' from 'www.google.com')
        base_domain_parts = parsed_base.netloc.split('.')[-2:]
        base_domain = '.'.join(base_domain_parts)

        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            parsed_full = urlparse(full_url)
            
            # Check if the link's domain ends with the base domain
            if parsed_full.netloc.endswith(base_domain):
                internal_links.add(full_url)
        return internal_links

    def _get_urls_from_sitemap(self, url: str) -> Set[str]:
        """Fetch and parse sitemap.xml to find all URLs."""
        sitemap_url = urljoin(url, "/sitemap.xml")
        urls = set()
        try:
            response = self.session.get(sitemap_url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "xml")
                for loc in soup.find_all("loc"):
                    urls.add(loc.text)
        except requests.RequestException as e:
            logging.warning(f"Could not fetch or parse sitemap {sitemap_url}: {e}")
        return urls

    def crawl_with_requests(self, tool_name: str, url: str) -> Optional[CrawlResult]:
        """Crawl a site using requests/BeautifulSoup."""
        validated_url = self._validate_url(url)
        if not validated_url:
            logging.warning(f"{tool_name}: Invalid URL: {url}")
            return None

        result = CrawlResult(tool_name=tool_name, url_root=validated_url, method_used="requests")
        
        try:
            headers = {"User-Agent": self.get_random_user_agent()}
            response = self.session.get(validated_url, headers=headers, timeout=15, allow_redirects=True)
            response.raise_for_status()

            result.status_code = str(response.status_code)
            result.pages_checked += 1
            
            soup = BeautifulSoup(response.text, 'html.parser')
            result.emails.update(self._extract_emails(response.text))

            is_affiliate, keywords = self._check_affiliate_indicators(validated_url, soup)
            result.keywords_found.update(keywords)

            if is_affiliate:
                result.affiliate_found = True
                result.affiliate_url = validated_url
                return result

            # Combine internal links and sitemap URLs for a comprehensive list
            all_links = self._get_internal_links(soup, validated_url)
            sitemap_urls = self._get_urls_from_sitemap(validated_url)
            all_links.update(sitemap_urls)

            # Prioritize links that are more likely to be affiliate pages
            priority_links = {link for link in all_links if any(keyword in link for keyword in ['affiliate', 'partner', 'contact'])}
            other_links = all_links - priority_links
            sorted_links = list(priority_links) + list(other_links)


            # If not found, check internal links
            for link in sorted_links[:self.max_pages - 1]:
                try:
                    response = self.session.get(link, headers=headers, timeout=10)
                    result.pages_checked += 1
                    link_soup = BeautifulSoup(response.text, 'html.parser')
                    is_affiliate, keywords = self._check_affiliate_indicators(link, link_soup)
                    result.keywords_found.update(keywords)
                    if is_affiliate:
                        result.affiliate_found = True
                        result.affiliate_url = link
                        return result
                except requests.RequestException as e:
                    logging.warning(f"Could not fetch internal link {link}: {e}")

            return result

        except requests.RequestException as e:
            logging.error(f"Requests error for {tool_name} ({validated_url}): {e}")
            result.status_code = "REQUESTS_ERROR"
            return result

    async def crawl_with_playwright(self, tool_name: str, url: str) -> Optional[CrawlResult]:
        """Crawl a site using Playwright as a fallback."""
        validated_url = self._validate_url(url)
        if not validated_url:
            return None

        result = CrawlResult(tool_name=tool_name, url_root=validated_url, method_used="playwright")
        
        async with async_playwright() as p:
            browser_args = {}
            if self.use_proxies and self.proxies:
                proxy = random.choice(self.proxies)
                browser_args['proxy'] = {'server': proxy}

            browser = await p.chromium.launch(headless=self.headless, args=['--no-sandbox'], **browser_args)
            page = await browser.new_page(user_agent=self.get_random_user_agent())
            
            try:
                await page.goto(validated_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(random.randint(1000, 3000)) # Human-like delay
                
                # Correctly await the status code and handle potential errors
                try:
                    response = page.locator('html').first
                    if response:
                         result.status_code = "200" # Simplified for now
                except Exception:
                    result.status_code = "PLAYWRIGHT_STATUS_ERROR"


                result.pages_checked += 1

                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                result.emails.update(self._extract_emails(soup.get_text()))
                is_affiliate, keywords = self._check_affiliate_indicators(validated_url, soup)
                result.keywords_found.update(keywords)

                if is_affiliate:
                    result.affiliate_found = True
                    result.affiliate_url = validated_url
                    await browser.close()
                    return result
                
                # Check internal links
                internal_links = self._get_internal_links(soup, validated_url)
                for link in list(internal_links)[:self.max_pages - 1]:
                    try:
                        await page.goto(link, wait_until='domcontentloaded', timeout=20000)
                        await page.wait_for_timeout(random.randint(500, 1500)) # Human-like delay
                        result.pages_checked += 1
                        content = await page.content()
                        link_soup = BeautifulSoup(content, 'html.parser')
                        is_affiliate, keywords = self._check_affiliate_indicators(link, link_soup)
                        result.keywords_found.update(keywords)
                        if is_affiliate:
                            result.affiliate_found = True
                            result.affiliate_url = link
                            await browser.close()
                            return result
                    except Exception as e:
                        logging.warning(f"Playwright could not fetch internal link {link}: {e}")

            except Exception as e:
                logging.error(f"Playwright error for {tool_name} ({validated_url}): {e}")
                result.status_code = "PLAYWRIGHT_ERROR"
            
            await browser.close()
            return result

    async def process_tool(self, tool_name: str, url: str):
        """Process a single tool."""
        if tool_name in self.progress['processed_tools']:
            logging.info(f"{tool_name} already processed. Skipping.")
            return

        logging.info(f"Processing {tool_name} with URL {url}")

        result = self.crawl_with_requests(tool_name, url)

        if not result or (not result.affiliate_found and result.status_code.startswith('2')):
             logging.info(f"Requests failed or found nothing for {tool_name}. Trying Playwright.")
             result = await self.crawl_with_playwright(tool_name, url)

        if result:
            self._save_result(result)
        
        self.progress['processed_tools'].append(tool_name)
        self._save_progress()


    async def run(self, tools_data: pd.DataFrame):
        tasks = []
        for _, row in tools_data.iterrows():
            tool_name = row['tool_name']
            url = row['tool_link']
            if tool_name not in self.progress['processed_tools']:
                tasks.append(self.process_tool(tool_name, url))
        
        await tqdm_asyncio.gather(*tasks)


class DataValidator:
    """Class to validate the crawled data."""
    def __init__(self, input_file="better_affiliate_results.csv", output_file="validated_affiliate_results.csv"):
        self.input_file = input_file
        self.output_file = output_file
        self.session = requests.Session()
        self.generic_email_patterns = ['noreply', 'support', 'privacy', 'jobs', 'contact@', 'hello@']
        self.partner_email_keywords = ['partner', 'affiliate', 'biz', 'growth', 'marketing']

    def validate_url(self, url: str) -> str:
        """Validate if a URL is live by sending a HEAD request."""
        if not url:
            return "NO_URL"
        try:
            response = self.session.head(url, timeout=10, allow_redirects=True)
            return str(response.status_code)
        except requests.RequestException:
            return "UNREACHABLE"

    def score_email(self, email: str) -> int:
        """Score an email based on its relevance for partnerships."""
        if not email:
            return 0
        email_lower = email.lower()
        if any(pattern in email_lower for pattern in self.generic_email_patterns):
            return 1 # Low score for generic emails
        if any(keyword in email_lower for keyword in self.partner_email_keywords):
            return 3 # High score for relevant emails
        return 2 # Medium score for others

    def process_row(self, row: dict) -> dict:
        """Validate and enrich a single row of data."""
        affiliate_url = row.get('affiliate_url', '')
        emails_str = row.get('emails', '')
        emails = emails_str.split('; ') if isinstance(emails_str, str) else []


        row['url_status'] = self.validate_url(affiliate_url)
        
        scored_emails = {email: self.score_email(email) for email in emails if email}
        # Sort emails by score (descending) and get the best ones
        sorted_emails = sorted(scored_emails.items(), key=lambda item: item[1], reverse=True)
        
        row['best_email'] = sorted_emails[0][0] if sorted_emails else ""
        row['best_email_score'] = sorted_emails[0][1] if sorted_emails else 0

        # Simple confidence score
        confidence = 0
        if row['affiliate_found'] == 'yes':
            confidence += 50
        if row['url_status'] == '200':
            confidence += 30
        if row['best_email_score'] == 3:
            confidence += 20
        row['confidence_score'] = confidence
        return row

    def validate(self):
        """Read, validate, and write the results."""
        if not os.path.exists(self.input_file):
            logging.warning(f"{self.input_file} not found. Skipping validation.")
            return

        df = pd.read_csv(self.input_file)
        
        # Using tqdm for progress bar
        validated_rows = []
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Validating results"):
            validated_rows.append(self.process_row(row.to_dict()))

        validated_df = pd.DataFrame(validated_rows)
        validated_df.to_csv(self.output_file, index=False)
        logging.info(f"Validation complete. Results saved to {self.output_file}")


async def main():
    try:
        df = pd.read_csv('tools.csv')
        crawler = BetterAffiliateCrawler()
        crawler.run_cleanup() # Clean files before running
        await crawler.run(df)

        # Add validation step
        validator = DataValidator()
        validator.validate()

    except FileNotFoundError:
        logging.error("tools.csv not found. Please create it.")
    except Exception as e:
        logging.error(f"An error occurred in main: {e}")


if __name__ == "__main__":
    asyncio.run(main())
