"""
UNIFIED MOBILE CRAWLER - FIXED VERSION WITH ENHANCED ERROR HANDLING
=====================================================================
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import re
from datetime import datetime
import os
import sys
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
from pathlib import Path
import pymysql
from pymysql.cursors import DictCursor
from config.controller_setting import get_db_controller_url
from config.crawler_setting import get_crawler_settings


def get_db_config():
    connection_url = get_db_controller_url()
    parsed = urlparse(connection_url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 3306,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/'),
        'charset': 'utf8mb4'
    }


def get_connection():
    connection_url = get_db_controller_url()
    parsed = urlparse(connection_url)
    return pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )


class ConfigLoader:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config.copy()
        self.db_config.setdefault('port', 3306)
        self.db_config.setdefault('charset', 'utf8mb4')
        self.db_config['cursorclass'] = DictCursor
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.conn = pymysql.connect(**self.db_config)
            print("✅ Đã kết nối Control DB")
        except pymysql.Error as e:
            print(f"❌ Lỗi kết nối: {e}")
            raise ConnectionError(f"Không thể kết nối: {e}")

    def close(self) -> None:
        if self.conn and self.conn.open:
            self.conn.close()

    def load_site_config(self, site_name: str) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM crawl_site_config WHERE site_name = %s AND is_active = TRUE"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (site_name,))
                return cursor.fetchone()
        except pymysql.Error:
            return None

    def load_selectors(self, site_id: int) -> Dict[str, List[str]]:
        query = "SELECT selector_key, selector_values FROM crawl_selector_config WHERE site_id = %s ORDER BY priority"
        selectors = {}
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (site_id,))
                for row in cursor.fetchall():
                    try:
                        selectors[row['selector_key']] = json.loads(row['selector_values'])
                    except json.JSONDecodeError:
                        pass
        except pymysql.Error:
            pass
        return selectors

    def load_field_mapping(self, site_id: int) -> Dict[str, Dict[str, Any]]:
        query = "SELECT field_name, mapping_keywords, regex_pattern FROM crawl_field_mapping WHERE site_id = %s"
        mappings = {}
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (site_id,))
                for row in cursor.fetchall():
                    try:
                        keywords = json.loads(row['mapping_keywords'])
                    except json.JSONDecodeError:
                        keywords = []
                    mappings[row['field_name']] = {
                        'keywords': keywords,
                        'regex': row['regex_pattern']
                    }
        except pymysql.Error:
            pass
        return mappings

    def get_complete_config(self, site_name: str) -> Optional[Dict[str, Any]]:
        site_config = self.load_site_config(site_name)
        if not site_config:
            return None
        site_id = site_config['site_id']
        return {
            'site': site_config,
            'selectors': self.load_selectors(site_id),
            'field_mapping': self.load_field_mapping(site_id),
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class BaseCrawler(ABC):
    def __init__(self, site_name: str, db_config: Dict = None):
        self.source_name = site_name
        self.driver = None
        self.wait = None
        self.max_retries = 3
        self.retry_delay = 5

        if db_config:
            config_loader = ConfigLoader(db_config)
            self.config = config_loader.get_complete_config(site_name)
            config_loader.close()

            if self.config:
                self.site_config = self.config['site']
                self.selectors = self.config['selectors']
                self.field_mapping = self.config['field_mapping']
                print(f"✅ Đã load config từ DB cho: {site_name}")
            else:
                self.config = None
                self.site_config = {}
                self.selectors = {}
                self.field_mapping = {}
        else:
            self.config = None
            self.site_config = {}
            self.selectors = {}
            self.field_mapping = {}

    def find_element_with_selectors(self, selector_key: str, timeout: int = 10):
        selectors_list = self.selectors.get(selector_key, [])
        for selector in selectors_list:
            try:
                if selector.startswith('//'):
                    element = WebDriverWait(self.driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                else:
                    element = WebDriverWait(self.driver, timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                if element:
                    return element
            except (NoSuchElementException, TimeoutException):
                continue
            except Exception:
                continue
        return None

    def safe_get_page(self, url: str, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            try:
                print(f"  🔄 Đang load trang (lần {attempt + 1}/{max_retries})...")
                self.driver.set_page_load_timeout(60)
                self.driver.get(url)
                time.sleep(3)
                return True
            except TimeoutException:
                print(f"  ⏱️ Timeout (lần {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                    try:
                        self.driver.execute_script("window.stop();")
                        return True
                    except:
                        pass
            except WebDriverException as e:
                print(f"  ❌ WebDriver error (lần {attempt + 1}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
            except Exception as e:
                print(f"  ❌ Lỗi (lần {attempt + 1}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
        return False

    @abstractmethod
    def get_all_product_links(self):
        pass

    @abstractmethod
    def crawl_single_product(self, product_url):
        pass

def import_json_to_mysql(json_files: List[str] = None) -> None:
    """
    Import 1 hoặc nhiều file JSON vào MySQL
    Format file: <nguon>_<YYYYMMDD>_<HHMMSS>.json
    VD: cellphones_20241123_143052.json, thegioididong_20241123_143052.json
    """
    if json_files is None:
        # Tự động tìm các file JSON với pattern mới
        import glob
        json_files = (
                glob.glob("cellphones_*_*.json") +
                glob.glob("thegioididong_*_*.json")
        )

        if not json_files:
            print("❌ Không tìm thấy file JSON nào!")
            print("   Format: <nguon>_<YYYYMMDD>_<HHMMSS>.json")
            print("   VD: cellphones_20241123_143052.json")
            return

        # Sắp xếp theo thời gian (mới nhất trước)
        json_files.sort(reverse=True)

        print(f"📁 Tìm thấy {len(json_files)} file JSON:")
        for f in json_files:
            print(f"   - {f}")

    # Đảm bảo json_files là list
    if isinstance(json_files, str):
        json_files = [json_files]

    total_imported = 0
    file_stats = []

    for json_file in json_files:
        json_path = Path(json_file)

        if not json_path.exists():
            print(f"❌ File không tồn tại: {json_file}")
            continue

        print(f"\n📖 Đang đọc file: {json_path.name}")

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                print(f"⚠️ File rỗng: {json_file}")
                continue

            print(f"   ✅ Đã load {len(data)} sản phẩm")

            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()

                sql = """
                      INSERT INTO general (product_name, price, screen_tech, camera_back, camera_front, \
                                           chip, sim, network_support, ram, rom, battery, \
                                           os, ip_rating, url, source) \
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                      """

                rows = []
                for item in data:
                    rows.append((
                        item.get("Tên sản phẩm"),
                        item.get("Giá"),
                        item.get("Công nghệ màn hình"),
                        item.get("Cam sau"),
                        item.get("Cam trước"),
                        item.get("Chip"),
                        item.get("Sim"),
                        item.get("Hỗ trợ mạng"),
                        item.get("RAM"),
                        item.get("ROM"),
                        item.get("Pin"),
                        item.get("Hệ điều hành"),
                        item.get("Kháng nước bụi"),
                        item.get("URL"),
                        item.get("Nguồn"),
                    ))

                print(f"   ⏳ Đang insert vào database...")
                cursor.executemany(sql, rows)
                conn.commit()

                imported_count = cursor.rowcount
                total_imported += imported_count
                file_stats.append((json_path.name, imported_count))
                print(f"   ✅ Import thành công {imported_count} sản phẩm")

            except Exception as e:
                print(f"   ❌ Lỗi khi import: {e}")
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    cursor.close()
                    conn.close()

        except json.JSONDecodeError as e:
            print(f"❌ Lỗi đọc JSON {json_file}: {e}")
        except Exception as e:
            print(f"❌ Lỗi không xác định với {json_file}: {e}")

    # Thống kê
    print("\n" + "=" * 70)
    print("🎉 HOÀN THÀNH IMPORT!")
    print("=" * 70)
    for filename, count in file_stats:
        print(f"   📄 {filename:40s} → {count:4d} sản phẩm")
    print("-" * 70)
    print(f"   📊 Tổng cộng: {total_imported} sản phẩm")
    print("=" * 70)

class CellphonesCrawler(BaseCrawler):
    def __init__(self, db_config: Dict = None):
        super().__init__("CellphoneS", db_config)
        if not self.selectors:
            self.selectors = {
                'product_name': ['.box-product-name h1'],
                'price': ['.sale-price'],
                'specs_container': ['table.technical-content'],
            }

    def get_all_product_links(self):
        print(f"\n🔍 [{self.source_name}] Đang lấy danh sách sản phẩm...")
        list_url = self.site_config.get('list_page_url', 'https://cellphones.com.vn/mobile.html')

        if not self.safe_get_page(list_url):
            return []

        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.filter-sort__list-product"))
            )
        except:
            pass

        product_links = []
        try:
            max_scroll = self.site_config.get('max_scroll_attempts', 10)
            self.scroll_to_load_all_products(max_scroll)

            link_selectors = self.selectors.get('product_link_selectors', ['.product-info > a'])
            elements = []
            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        break
                except:
                    continue

            for element in elements:
                href = element.get_attribute("href")
                if href and self.is_valid_product_link(href):
                    if href not in product_links:
                        product_links.append(href)

            print(f"  ✅ Tìm thấy {len(product_links)} link")
            return product_links
        except Exception as e:
            print(f"  ❌ Lỗi: {e}")
            return []

    def is_valid_product_link(self, href):
        if not href:
            return False
        pattern = self.site_config.get('link_validation_pattern')
        if pattern:
            return bool(re.search(pattern, href, re.IGNORECASE))

        href = href.lower()
        invalid = ["bo-loc", "mobile/", "sforum", "tin-tuc"]
        if any(x in href for x in invalid):
            return False

        keywords = ["dien-thoai", "iphone", "samsung", "xiaomi", "oppo", "tecno", "honor", "nubia", "sony", "nokia",
                    "vivo", "realme", "oneplus"]
        return any(k in href for k in keywords)

    def scroll_to_load_all_products(self, max_clicks=10):
        wait_time = self.site_config.get('scroll_wait_time', 5)
        for i in range(max_clicks):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                show_more_selectors = self.selectors.get('show_more_button', [".cps-block-content_btn-showmore > a"])
                show_more_btn = None
                for selector in show_more_selectors:
                    try:
                        show_more_btn = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        if show_more_btn:
                            break
                    except:
                        continue

                if show_more_btn:
                    self.driver.execute_script("arguments[0].click();", show_more_btn)
                    time.sleep(wait_time)
                else:
                    break
            except:
                break

    def crawl_single_product(self, product_url):
        if not self.safe_get_page(product_url):
            return None

        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".box-product-name h1"))
            )

            product_data = {
                "Tên sản phẩm": None, "Giá": None, "Công nghệ màn hình": None,
                "Cam sau": None, "Cam trước": None, "Chip": None, "Sim": None,
                "Hỗ trợ mạng": None, "RAM": None, "ROM": None, "Pin": None,
                "Hệ điều hành": None, "Kháng nước bụi": None,
                "URL": product_url, "Nguồn": self.source_name,
            }

            name_elem = self.find_element_with_selectors('product_name', timeout=5)
            if name_elem:
                product_data["Tên sản phẩm"] = name_elem.text.strip()

            price_elem = self.find_element_with_selectors('price', timeout=5)
            if price_elem:
                product_data["Giá"] = price_elem.text.strip()

            try:
                modal_selectors = self.selectors.get('modal_button', ["button.button__show-modal-technical"])
                for selector in modal_selectors:
                    try:
                        btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                        self.driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
                        break
                    except:
                        continue
            except:
                pass

            specs_selectors = self.selectors.get('specs_table_rows',
                                                 ["table.technical-content tr.technical-content-item"])
            rows = []
            for selector in specs_selectors:
                try:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if rows:
                        break
                except:
                    continue

            for row in rows:
                try:
                    cols = row.find_elements(By.TAG_NAME, 'td')
                    if len(cols) >= 2:
                        key_raw = cols[0].text.strip()
                        value = cols[1].text.strip()
                        standard_key = self.map_field_key(key_raw)
                        if standard_key:
                            product_data[standard_key] = value
                except:
                    continue

            return product_data
        except Exception as e:
            print(f"  ❌ Lỗi: {str(e)[:100]}")
            return None

    def map_field_key(self, key_raw: str) -> Optional[str]:
        key_lower = key_raw.lower()
        for standard_field, mapping in self.field_mapping.items():
            keywords = mapping.get('keywords', [])
            regex = mapping.get('regex')
            if any(kw.lower() in key_lower for kw in keywords):
                return standard_field
            if regex and re.search(regex, key_raw, re.IGNORECASE):
                return standard_field
        return None


class TheGioiDiDongCrawler(BaseCrawler):
    def __init__(self, db_config: Dict = None):
        super().__init__("TheGioiDiDong", db_config)
        if not self.selectors:
            self.selectors = {
                'product_name': ['h1.txt-color-black', 'h1'],
                'price': ['.box-info__box-price .price', '.box-price .price'],
                'specs_container': ['.parameter__list', 'ul.parameter'],
            }

    def get_all_product_links(self):
        print(f"\n🔍 [{self.source_name}] Đang lấy danh sách sản phẩm...")
        list_url = self.site_config.get('list_page_url', 'https://www.thegioididong.com/dtdd')

        if not self.safe_get_page(list_url):
            return []

        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listproduct"))
            )
        except:
            pass

        product_links = []
        try:
            max_scroll = self.site_config.get('max_scroll_attempts', 10)
            self.scroll_to_load_all_products(max_scroll)

            link_selectors = self.selectors.get('product_link_selectors', ['li.item > a', 'li > a'])
            elements = []
            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        break
                except:
                    continue

            for element in elements:
                try:
                    href = element.get_attribute("href")
                    if href and self.is_valid_product_link(href):
                        if href not in product_links:
                            product_links.append(href)
                except:
                    continue

            print(f"  ✅ Tìm thấy {len(product_links)} link")
            return product_links
        except Exception as e:
            print(f"  ❌ Lỗi: {e}")
            return []

    def is_valid_product_link(self, href):
        if not href:
            return False
        pattern = self.site_config.get('link_validation_pattern')
        if pattern:
            return bool(re.search(pattern, href, re.IGNORECASE))

        href = href.lower()
        invalid = ["loc-", "dtdd/", "dtdd#", "tin-tuc"]
        if any(x in href for x in invalid):
            return False

        keywords = ["dtdd-", "iphone", "samsung", "xiaomi", "oppo", "tecno", "honor", "nubia", "sony", "nokia", "vivo",
                    "realme", "oneplus"]
        return any(k in href for k in keywords)

    def scroll_to_load_all_products(self, max_clicks=10):
        wait_time = self.site_config.get('scroll_wait_time', 5)
        for i in range(max_clicks):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                show_more_selectors = self.selectors.get('show_more_button', [".view-more a", "a.view-more"])
                show_more_btn = None
                for selector in show_more_selectors:
                    try:
                        show_more_btn = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        if show_more_btn:
                            break
                    except:
                        continue

                if show_more_btn:
                    self.driver.execute_script("arguments[0].click();", show_more_btn)
                    time.sleep(wait_time)
                else:
                    break
            except:
                break

    def crawl_single_product(self, product_url):
        if not self.safe_get_page(product_url):
            return None

        try:
            element_found = False
            for selector in ['h1.txt-color-black', 'h1', '.box-info']:
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    element_found = True
                    break
                except:
                    continue

            product_data = {
                "Tên sản phẩm": None, "Giá": None, "Công nghệ màn hình": None,
                "Cam sau": None, "Cam trước": None, "Chip": None, "Sim": None,
                "Hỗ trợ mạng": None, "RAM": None, "ROM": None, "Pin": None,
                "Hệ điều hành": None, "Kháng nước bụi": None,
                "URL": product_url, "Nguồn": self.source_name,
            }

            name_elem = self.find_element_with_selectors('product_name', timeout=10)
            if name_elem:
                product_data["Tên sản phẩm"] = name_elem.text.strip()

            price_elem = self.find_element_with_selectors('price', timeout=10)
            if price_elem:
                product_data["Giá"] = price_elem.text.strip()

            specs_selectors = self.selectors.get('specs_table_rows', [".parameter__list li", "ul.parameter li"])
            rows = []
            for selector in specs_selectors:
                try:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if rows:
                        break
                except:
                    continue

            for row in rows:
                try:
                    key_raw = row.find_element(By.CSS_SELECTOR, 'p').text.strip()
                    value = row.find_element(By.CSS_SELECTOR, 'div').text.strip()
                    standard_key = self.map_field_key(key_raw)
                    if standard_key:
                        product_data[standard_key] = value
                except:
                    continue

            return product_data
        except Exception as e:
            print(f"  ❌ Lỗi: {str(e)[:100]}")
            return None

    def map_field_key(self, key_raw: str) -> Optional[str]:
        key_lower = key_raw.lower()
        for standard_field, mapping in self.field_mapping.items():
            keywords = mapping.get('keywords', [])
            regex = mapping.get('regex')
            if any(kw.lower() in key_lower for kw in keywords):
                return standard_field
            if regex and re.search(regex, key_raw, re.IGNORECASE):
                return standard_field
        return None


class UnifiedCrawlerManager:
    def __init__(self, headless: bool = False, save_checkpoint: bool = True, use_db_config: bool = False):
        # Load crawler settings
        self.crawler_settings = get_crawler_settings()

        self.headless = headless if headless else self.crawler_settings['headless']
        self.save_checkpoint = save_checkpoint if save_checkpoint else self.crawler_settings['save_checkpoint']
        self.user_agent = self.crawler_settings['user_agent']

        self.use_db_config = use_db_config
        self.driver = None
        self.all_products = []
        self.checkpoint_file = 'crawler_checkpoint.json'
        self.driver_restart_interval = 30
        self.products_crawled_count = 0

        db_config = get_db_config() if use_db_config else None
        self.crawlers = {
            'cellphones': CellphonesCrawler(db_config),
            'tgdd': TheGioiDiDongCrawler(db_config)
        }

        if self.load_checkpoint():
            print("✅ Đã load checkpoint thành công")

        if use_db_config:
            self.etl_logger = ETLLogger(get_db_config())
        else:
            self.etl_logger = None

    def start_driver(self):
        print("\n🚀 Khởi động browser với anti-detection...")
        options = webdriver.ChromeOptions()

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins-discovery")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument(f"--user-agent={self.user_agent}")

        if self.headless:
            options.add_argument("--headless=new")
            print("  ℹ️ Chạy headless mode")
        else:
            print("  ℹ️ Chạy không headless")

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false,
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['vi-VN', 'vi', 'en-US', 'en'],
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                window.chrome = {
                    runtime: {},
                    loadTimes: () => undefined,
                    csi: () => undefined,
                };
                delete navigator.__proto__.webdriver;
            """
        })

        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(60)

        for crawler in self.crawlers.values():
            crawler.driver = self.driver
            crawler.wait = WebDriverWait(self.driver, 20)

        print("✅ Browser ready!")

    def restart_driver(self):
        print("\n🔄 Restarting driver...")
        self.close_driver()
        time.sleep(3)
        self.start_driver()
        self.products_crawled_count = 0

    def close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
                print("✔ Đã đóng browser")
            except:
                pass
            self.driver = None

    def load_checkpoint(self) -> bool:
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.all_products = data.get('products', [])
                    return True
            except Exception as e:
                print(f"❌ Lỗi load checkpoint: {e}")
                return False
        return False

    def save_checkpoint_data(self, crawled_links: List[str], source: str):
        if not self.save_checkpoint:
            return

        checkpoint = {
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'crawled_links': crawled_links,
            'products': self.all_products
        }

        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        print("💾 Đã lưu checkpoint")

    def crawl_source(self, source_key: str, max_products: Optional[int] = None) -> int:
        if source_key not in self.crawlers:
            print(f"❌ Nguồn không hợp lệ: {source_key}")
            return 0

        crawler = self.crawlers[source_key]
        print("\n" + "=" * 70)
        print(f"🌐 BẮT ĐẦU CRAWL: {crawler.source_name.upper()}")
        print("=" * 70)

        product_links = crawler.get_all_product_links()

        if max_products:
            product_links = product_links[:max_products]

        success_count = 0
        fail_count = 0
        consecutive_fails = 0
        crawled_links = []

        for idx, product_url in enumerate(product_links, 1):
            if self.products_crawled_count >= self.driver_restart_interval:
                print(f"\n⚠️ Đã crawl {self.products_crawled_count} sản phẩm, restart driver...")
                self.restart_driver()
                consecutive_fails = 0

            if consecutive_fails >= 5:
                print(f"\n⚠️ Đã fail {consecutive_fails} lần liên tiếp, restart driver...")
                self.restart_driver()
                consecutive_fails = 0

            print(f"\n  [{idx}/{len(product_links)}] Crawl: {product_url}")

            product_data = None
            for retry in range(3):
                try:
                    product_data = crawler.crawl_single_product(product_url)
                    if product_data:
                        break
                except WebDriverException as e:
                    print(f"  ⚠️ WebDriver error (retry {retry + 1}/3): {str(e)[:50]}")
                    if retry < 2:
                        print("  🔄 Restart driver...")
                        self.restart_driver()
                        time.sleep(3)
                except Exception as e:
                    print(f"  ⚠️ Error (retry {retry + 1}/3): {str(e)[:50]}")
                    if retry < 2:
                        time.sleep(5)

            if product_data:
                self.all_products.append(product_data)
                success_count += 1
                crawled_links.append(product_url)
                self.products_crawled_count += 1
                consecutive_fails = 0

                name = product_data.get('Tên sản phẩm', 'N/A')
                price = product_data.get('Giá', 'N/A')
                chip = product_data.get('Chip', 'N/A')

                print(f"  ✅ Tên: {name}")
                print(f"     Giá: {price} | Chip: {chip}")
            else:
                fail_count += 1
                consecutive_fails += 1
                print(f"  ❌ Thất bại (consecutive: {consecutive_fails})")

            time.sleep(2)

            if idx % 10 == 0:
                self.save_checkpoint_data(crawled_links, crawler.source_name)

        print("\n" + "=" * 70)
        print(f"✅ [{crawler.source_name}] Hoàn thành: {success_count} thành công | ❌ {fail_count} thất bại")
        print("=" * 70)

        return success_count

    def crawl_all_sources(self, sources=None, max_products_per_source=None):
        if sources is None:
            sources = list(self.crawlers.keys())

        print("\n" + "=" * 70)
        print("🌐 CRAWL TỪ NHIỀU NGUỒN")
        print("=" * 70)
        print(f"Nguồn: {', '.join([self.crawlers[s].source_name for s in sources])}")
        print("=" * 70)

        # THÊM MỚI: Bắt đầu ETL batch
        if self.etl_logger:
            source_names = ', '.join([self.crawlers[s].source_name for s in sources])
            self.etl_logger.start_batch(source_names)
            self.etl_logger.start_process('Extract')  # Process ID = 1

        total_success = 0

        try:
            for source_name in sources:
                success = self.crawl_source(source_name, max_products=max_products_per_source)
                total_success += success

                # THÊM MỚI: Log progress sau mỗi nguồn
                if self.etl_logger:
                    self.etl_logger.log_progress(records_inserted=success)

                if len(sources) > 1:
                    print("\n🔄 Chuyển nguồn → restart driver...")
                    self.restart_driver()
                    time.sleep(3)

            # THÊM MỚI: Kết thúc process Extract thành công
            if self.etl_logger:
                self.etl_logger.end_process('success')

        except Exception as e:
            # THÊM MỚI: Log lỗi nếu có
            if self.etl_logger:
                self.etl_logger.end_process('failed', str(e))
            raise

        return total_success

    # def save_results(self):
    #     """Lưu kết quả với format: <nguon>_<YYYYMMDD>_<HHMMSS>.json"""
    #     now = datetime.now()
    #     date_str = now.strftime("%Y%m%d")
    #     time_str = now.strftime("%H%M%S")
    #
    #     # Phân loại sản phẩm theo nguồn
    #     products_by_source = {}
    #     for product in self.all_products:
    #         source = product.get('Nguồn', 'Unknown')
    #         if source not in products_by_source:
    #             products_by_source[source] = []
    #         products_by_source[source].append(product)
    #
    #     saved_files = []
    #     for source, products in products_by_source.items():
    #         # Chuẩn hóa tên nguồn
    #         source_clean = source.lower().replace(" ", "")
    #         if 'cellphones' in source_clean:
    #             source_name = "cellphones"
    #         elif 'thegioididong' in source_clean:
    #             source_name = "thegioididong"
    #         else:
    #             source_name = source_clean
    #
    #         # Format mới: <nguon>_<YYYYMMDD>_<HHMMSS>.json
    #         filename = f"{source_name}_{date_str}_{time_str}.json"
    #
    #         with open(filename, 'w', encoding='utf-8') as f:
    #             json.dump(products, f, ensure_ascii=False, indent=2)
    #
    #         saved_files.append((filename, len(products)))
    #         print(f"✅ Saved: {filename} ({len(products)} sản phẩm)")
    #
    #     print("\n📁 File đã lưu:")
    #     print("-" * 70)
    #     for filename, count in saved_files:
    #         print(f"  {filename:45s} {count:4d} sản phẩm")
    #
    #     if os.path.exists(self.checkpoint_file):
    #         os.remove(self.checkpoint_file)
    #         print("\n✔ Đã xóa checkpoint file")

    def save_results(self):
        """Lưu kết quả với format: cellphones.json và tgdd.json"""
        # Phân loại sản phẩm theo nguồn
        products_by_source = {}
        for product in self.all_products:
            source = product.get('Nguồn', 'Unknown')
            if source not in products_by_source:
                products_by_source[source] = []
            products_by_source[source].append(product)

        saved_files = []
        for source, products in products_by_source.items():
            # Chuẩn hóa tên nguồn
            source_clean = source.lower().replace(" ", "")
            if 'cellphones' in source_clean:
                filename = "../crawed/cellphones.json"
            elif 'thegioididong' in source_clean:
                filename = "../crawed/tgdd.json"
            else:
                filename = f"{source_clean}.json"

            # Lưu file JSON
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)

            saved_files.append((filename, len(products)))
            print(f"✅ Saved: {filename} ({len(products)} sản phẩm)")

        print("\n📁 File đã lưu:")
        print("-" * 70)
        for filename, count in saved_files:
            print(f"  {filename:45s} {count:4d} sản phẩm")

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
            print("\n✔ Đã xóa checkpoint file")


class ETLLogger:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
        self.batch_id = None
        self.current_process_id = None

    def _get_connection(self):
        """Tạo connection mới cho logger"""
        return pymysql.connect(
            host=self.db_config['host'],
            port=self.db_config.get('port', 3306),
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )

    def start_batch(self, source_name: str) -> int:
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO etl_log (source_table, batch_id, status) VALUES (%s, UUID(), 'started')",
                    (source_name,)
                )
                cursor.execute("SELECT LAST_INSERT_ID() AS id")
                result = cursor.fetchone()
                self.batch_id = result['id']
                conn.commit()
                print(f"Bắt đầu batch_id: {self.batch_id} | source: {source_name}")
                return self.batch_id
        finally:
            conn.close()

    def start_process(self, process_name: str) -> bool:
        if not self.batch_id:
            return False
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT process_id FROM process WHERE process_name = %s", (process_name,))
                row = cursor.fetchone()
                if not row:
                    print(f"Không tìm thấy process: {process_name}")
                    return False
                process_id = row['process_id']
                cursor.execute("""
                               UPDATE etl_log
                               SET process_id = %s,
                                   status     = 'running',
                                   start_time = NOW()
                               WHERE etl_id = %s
                               """, (process_id, self.batch_id))
                conn.commit()
                print(f"Bắt đầu process: {process_name} (ID: {process_id})")
                return True
        finally:
            conn.close()

    def log_progress(self, records_inserted: int = 0, records_updated: int = 0,
                     records_skipped: int = 0, message: str = None):
        """Cập nhật tiến độ trong quá trình chạy"""
        if not self.batch_id:
            return

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                update_parts = ["records_inserted = records_inserted + %s",
                                "records_updated = records_updated + %s",
                                "records_skipped = records_skipped + %s"]
                params = [records_inserted, records_updated, records_skipped]

                if message:
                    update_parts.append("error_message = %s")
                    params.append(message)

                params.append(self.batch_id)

                sql = f"UPDATE etl_log SET {', '.join(update_parts)} WHERE etl_id = %s"
                cursor.execute(sql, params)
                conn.commit()
        finally:
            conn.close()

    def end_process(self, status: str = 'success', error_message: str = None):
        """Kết thúc process"""
        if not self.batch_id:
            return

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE etl_log
                       SET status        = %s,
                           end_time      = NOW(),
                           error_message = %s
                       WHERE etl_id = %s""",
                    (status, error_message, self.batch_id)
                )
                conn.commit()

                if status == 'success':
                    print(f"✅ Hoàn thành process thành công")
                else:
                    print(f"❌ Process kết thúc với lỗi: {error_message}")
        finally:
            conn.close()

def auto_crawl():
    """Hàm chạy tự động khi script được gọi"""
    print("=" * 80)
    print("UNIFIED MOBILE CRAWLER - CHẾ ĐỘ TỰ ĐỘNG")
    print("Crawl CellphoneS + TheGioiDiDong → Lưu vào ../crawed/")
    print("=" * 80)
    print(f"⏰ Thời gian bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Đang khởi tạo crawler...")

    # Cấu hình tự động tối ưu cho Task Scheduler
    manager = UnifiedCrawlerManager(
        headless=True,           # BẮT BUỘC ẩn trình duyệt
        save_checkpoint=True,    # Luôn lưu checkpoint
        use_db_config=True       # Ưu tiên dùng config từ DB (nếu kết nối được)
    )

    # Tăng khoảng restart driver để ổn định hơn
    manager.driver_restart_interval = 40

    total_products = 0
    start_time = time.time()

    try:
        manager.start_driver()

        print("\nBẮT ĐẦU CRAWL TỰ ĐỘNG 2 NGUỒN...")
        total_success = manager.crawl_all_sources(
            sources=['cellphones', 'tgdd'],
            max_products_per_source = 120  # Crawl 120 sản phẩm từng nguồn
        )

        total_products = len(manager.all_products)

        if total_products > 0:
            manager.save_results()
            print(f"\nHOÀN TẤT! Đã crawl {total_products} sản phẩm")
        else:
            print("\nKHÔNG CRAWL ĐƯỢC SẢN PHẨM NÀO!")

    except KeyboardInterrupt:
        print("\nĐÃ DỪNG BỞI NGƯỜI DÙNG (Ctrl+C)")
    except Exception as e:
        print(f"\nLỖI NGHIÊM TRỌNG: {e}")
        import traceback
        traceback.print_exc()
    finally:
        manager.close_driver()
        elapsed = time.time() - start_time
        mins, secs = divmod(int(elapsed), 60)
        print(f"\nThời gian thực thi: {mins} phút {secs} giây")
        print(f"Hoàn thành lúc: {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)


if __name__ == "__main__":
    auto_crawl()