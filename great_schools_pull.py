import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
from urllib.parse import urljoin, quote
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SchoolDistrictData:
    """Data structure for school district information"""
    district_name: str
    county: str
    greatschools_rating: Optional[int] = None
    greatschools_url: Optional[str] = None
    niche_rating: Optional[str] = None
    niche_url: Optional[str] = None
    schooldigger_rating: Optional[str] = None
    schooldigger_url: Optional[str] = None
    enrollment: Optional[int] = None
    student_teacher_ratio: Optional[str] = None
    last_updated: Optional[str] = None

class BaseScraper:
    """Base class for all scrapers"""
    
    def __init__(self, delay: float = 1.0):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.delay = delay
    
    def get_page_source(self, url: str) -> Optional[str]:
        """Get full HTML source of a page"""
        try:
            time.sleep(self.delay)  # Rate limiting
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def save_source(self, url: str, filename: str):
        """Save page source to file for debugging"""
        source = self.get_page_source(url)
        if source:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(source)
            logger.info(f"Saved source to {filename}")

class GreatSchoolsScraper(BaseScraper):
    """Scraper for GreatSchools.org"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(delay=1.0)
        self.api_key = api_key
        self.base_url = "https://www.greatschools.org"
    
    def search_district(self, district_name: str, state: str = "PA") -> Optional[str]:
        """Search for district URL on GreatSchools"""
        search_url = f"{self.base_url}/search/search.page"
        params = {
            'q': district_name,
            'state': state,
            'level': 'district'
        }
        
        source = self.get_page_source(search_url + "?" + "&".join([f"{k}={quote(str(v))}" for k, v in params.items()]))
        if not source:
            return None
            
        soup = BeautifulSoup(source, 'html.parser')
        # Look for district links in search results
        district_links = soup.find_all('a', href=re.compile(r'/pennsylvania/.*-district/'))
        
        for link in district_links:
            if district_name.lower() in link.get_text().lower():
                return urljoin(self.base_url, link['href'])
        
        return None
    
    def scrape_district_data(self, district_url: str) -> Dict:
        """Scrape district data from GreatSchools page"""
        source = self.get_page_source(district_url)
        if not source:
            return {}
            
        soup = BeautifulSoup(source, 'html.parser')
        data = {}
        
        # Extract rating (typically in a rating badge)
        rating_elem = soup.find('div', class_=re.compile(r'rating|score'))
        if rating_elem:
            rating_text = rating_elem.get_text().strip()
            rating_match = re.search(r'(\d+)', rating_text)
            if rating_match:
                data['greatschools_rating'] = int(rating_match.group(1))
        
        # Extract enrollment
        enrollment_elem = soup.find(text=re.compile(r'students|enrollment', re.I))
        if enrollment_elem:
            parent = enrollment_elem.parent
            enrollment_text = parent.get_text() if parent else enrollment_elem
            enrollment_match = re.search(r'(\d+(?:,\d+)*)', enrollment_text)
            if enrollment_match:
                data['enrollment'] = int(enrollment_match.group(1).replace(',', ''))
        
        # Extract student-teacher ratio
        ratio_elem = soup.find(text=re.compile(r'student.*teacher|teacher.*student', re.I))
        if ratio_elem:
            ratio_match = re.search(r'(\d+:\d+)', ratio_elem.parent.get_text() if ratio_elem.parent else ratio_elem)
            if ratio_match:
                data['student_teacher_ratio'] = ratio_match.group(1)
        
        data['greatschools_url'] = district_url
        return data

class NicheScraper(BaseScraper):
    """Scraper for Niche.com"""
    
    def __init__(self):
        super().__init__(delay=1.5)
        self.base_url = "https://www.niche.com"
    
    def search_district(self, district_name: str, state: str = "pennsylvania") -> Optional[str]:
        """Search for district on Niche"""
        # Niche URLs are typically formatted as /k12/d/district-name-state/
        district_slug = district_name.lower().replace(' ', '-').replace('school-district', '').strip('-')
        potential_url = f"{self.base_url}/k12/d/{district_slug}-{state}/"
        
        # Test if URL exists
        source = self.get_page_source(potential_url)
        if source and "404" not in source:
            return potential_url
        
        # If direct URL doesn't work, try search
        search_url = f"{self.base_url}/search/k12/"
        params = {'q': f"{district_name} {state}"}
        
        source = self.get_page_source(search_url + "?" + "&".join([f"{k}={quote(str(v))}" for k, v in params.items()]))
        if not source:
            return None
            
        soup = BeautifulSoup(source, 'html.parser')
        district_links = soup.find_all('a', href=re.compile(r'/k12/d/.*pennsylvania'))
        
        for link in district_links:
            if district_name.lower() in link.get_text().lower():
                return urljoin(self.base_url, link['href'])
        
        return None
    
    def scrape_district_data(self, district_url: str) -> Dict:
        """Scrape district data from Niche page"""
        source = self.get_page_source(district_url)
        if not source:
            return {}
            
        soup = BeautifulSoup(source, 'html.parser')
        data = {}
        
        # Extract grade/rating (Niche uses letter grades)
        grade_elem = soup.find('div', class_=re.compile(r'grade|rating'))
        if grade_elem:
            grade_text = grade_elem.get_text().strip()
            grade_match = re.search(r'([A-F][+-]?)', grade_text)
            if grade_match:
                data['niche_rating'] = grade_match.group(1)
        
        data['niche_url'] = district_url
        return data

class SchoolDiggerScraper(BaseScraper):
    """Scraper for SchoolDigger.com"""
    
    def __init__(self):
        super().__init__(delay=1.0)
        self.base_url = "https://www.schooldigger.com"
    
    def search_district(self, district_name: str, state: str = "PA") -> Optional[str]:
        """Search for district on SchoolDigger"""
        search_url = f"{self.base_url}/go/PA/search.aspx"
        params = {
            'searchterm': district_name,
            'searchtype': 'district'
        }
        
        source = self.get_page_source(search_url + "?" + "&".join([f"{k}={quote(str(v))}" for k, v in params.items()]))
        if not source:
            return None
            
        soup = BeautifulSoup(source, 'html.parser')
        district_links = soup.find_all('a', href=re.compile(r'/go/PA/district\.aspx'))
        
        for link in district_links:
            if district_name.lower() in link.get_text().lower():
                return urljoin(self.base_url, link['href'])
        
        return None
    
    def scrape_district_data(self, district_url: str) -> Dict:
        """Scrape district data from SchoolDigger page"""
        source = self.get_page_source(district_url)
        if not source:
            return {}
            
        soup = BeautifulSoup(source, 'html.parser')
        data = {}
        
        # Extract ranking/rating
        ranking_elem = soup.find(text=re.compile(r'rank|rating', re.I))
        if ranking_elem:
            ranking_text = ranking_elem.parent.get_text() if ranking_elem.parent else ranking_elem
            ranking_match = re.search(r'(\d+)', ranking_text)
            if ranking_match:
                data['schooldigger_rating'] = ranking_match.group(1)
        
        data['schooldigger_url'] = district_url
        return data

class PASchoolDistrictScraper:
    """Main scraper class that coordinates all scrapers"""
    
    def __init__(self, greatschools_api_key: Optional[str] = None):
        self.greatschools_scraper = GreatSchoolsScraper(greatschools_api_key)
        self.niche_scraper = NicheScraper()
        self.schooldigger_scraper = SchoolDiggerScraper()
    
    def load_district_list(self, csv_file: str) -> List[str]:
        """Load district list from CSV file"""
        try:
            df = pd.read_csv(csv_file)
            # Assuming CSV has columns like 'district_name', 'county'
            return df['district_name'].tolist()
        except Exception as e:
            logger.error(f"Error loading district list: {e}")
            return []
    
    def scrape_all_districts(self, district_names: List[str], output_file: str = "pa_school_districts.csv"):
        """Scrape data for all districts"""
        results = []
        
        for i, district_name in enumerate(district_names):
            logger.info(f"Processing {district_name} ({i+1}/{len(district_names)})")
            
            district_data = SchoolDistrictData(
                district_name=district_name,
                county="",  # You can add county mapping logic here
                last_updated=pd.Timestamp.now().isoformat()
            )
            
            # Scrape GreatSchools
            gs_url = self.greatschools_scraper.search_district(district_name)
            if gs_url:
                gs_data = self.greatschools_scraper.scrape_district_data(gs_url)
                district_data.greatschools_rating = gs_data.get('greatschools_rating')
                district_data.greatschools_url = gs_data.get('greatschools_url')
                district_data.enrollment = gs_data.get('enrollment')
                district_data.student_teacher_ratio = gs_data.get('student_teacher_ratio')
            
            # Scrape Niche
            niche_url = self.niche_scraper.search_district(district_name)
            if niche_url:
                niche_data = self.niche_scraper.scrape_district_data(niche_url)
                district_data.niche_rating = niche_data.get('niche_rating')
                district_data.niche_url = niche_data.get('niche_url')
            
            # Scrape SchoolDigger
            sd_url = self.schooldigger_scraper.search_district(district_name)
            if sd_url:
                sd_data = self.schooldigger_scraper.scrape_district_data(sd_url)
                district_data.schooldigger_rating = sd_data.get('schooldigger_rating')
                district_data.schooldigger_url = sd_data.get('schooldigger_url')
            
            results.append(district_data)
            
            # Save intermediate results every 10 districts
            if (i + 1) % 10 == 0:
                self.save_results(results, f"temp_{output_file}")
        
        # Save final results
        self.save_results(results, output_file)
        logger.info(f"Scraping complete. Results saved to {output_file}")
        return results
    
    def save_results(self, results: List[SchoolDistrictData], filename: str):
        """Save results to CSV"""
        df = pd.DataFrame([vars(r) for r in results])
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(results)} districts to {filename}")

# Example usage
if __name__ == "__main__":
    # Test with a few districts
    test_districts = [
        "Philadelphia City School District",
        "Pittsburgh School District",
        "Central Dauphin School District"
    ]
    
    scraper = PASchoolDistrictScraper()
    
    # For testing - save source code of one page
    gs_scraper = GreatSchoolsScraper()
    test_url = gs_scraper.search_district("Philadelphia City School District")
    if test_url:
        gs_scraper.save_source(test_url, "philadelphia_greatschools_source.html")
    
    # Run full scraping
    # scraper.scrape_all_districts(test_districts, "test_results.csv")