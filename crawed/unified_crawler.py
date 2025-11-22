"""
UNIFIED MOBILE CRAWLER - CELL PHONES + TGDD
Crawl dữ liệu điện thoại từ 2 nguồn, lưu kết quả JSON
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time, json, re, os
from abc import ABC, abstractmethod


# ----------------------- BASE CRAWLER -----------------------
class BaseCrawler(ABC):
    def __init__(self, source_name):
        self.source_name = source_name
        self.driver = None
        self.wait = None

    @abstractmethod
    def get_all_product_links(self):
        pass

    @abstractmethod
    def crawl_single_product(self, product_url):
        pass

    def is_valid_product_link(self, href):
        return bool(href)


# ----------------------- CELLPHONES -----------------------
class CellphonesCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("CellphoneS")

    def get_all_product_links(self):
        print(f"[{self.source_name}] Lấy danh sách sản phẩm...")
        self.driver.get("https://cellphones.com.vn/mobile.html")
        try:
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.filter-sort__list-product"))
            )
        except TimeoutException:
            print("⛔ Load trang thất bại")
            return []

        self.scroll_to_load_all_products()

        links = []
        try:
            container = self.driver.find_element(By.CSS_SELECTOR, "#blockFilterSort > div.filter-sort__list-product")
            elements = container.find_elements(By.CSS_SELECTOR, ".product-info > a")
            for el in elements:
                href = el.get_attribute("href")
                if href and self.is_valid_product_link(href):
                    if href not in links:
                        links.append(href)
        except Exception as e:
            print(f"❌ Lỗi lấy link: {e}")

        print(f"✓ Tìm thấy {len(links)} sản phẩm")
        return links

    def is_valid_product_link(self, href):
        invalid_patterns = ["bo-loc", "mobile/", "sforum", "tin-tuc"]
        required_keywords = [
            "dien-thoai", "iphone", "samsung", "xiaomi", "oppo",
            "tecno", "honor", "nubia", "sony", "nokia", "vivo",
            "realme", "oneplus"
        ]
        return all(x not in href.lower() for x in invalid_patterns) and any(k in href.lower() for k in required_keywords)

    def scroll_to_load_all_products(self):
        for i in range(10):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                btn = self.driver.find_element(By.CSS_SELECTOR,
                    "#blockFilterSort > div.filter-sort__list-product > div > div.cps-block-content_btn-showmore > a")
                self.driver.execute_script("arguments[0].click();", btn)
                time.sleep(2)
            except:
                break

    def crawl_single_product(self, url):
        try:
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".box-product-name h1")))
        except:
            return None

        data = {
            "Tên sản phẩm": "Không tìm thấy", "Giá": "Không tìm thấy", "Công nghệ màn hình": "Không tìm thấy",
            "Cam sau": "Không tìm thấy", "Cam trước": "Không tìm thấy", "Chip": "Không tìm thấy",
            "Sim": "Không tìm thấy", "Hỗ trợ mạng": "Không tìm thấy", "RAM": "Không tìm thấy",
            "ROM": "Không tìm thấy", "Pin": "Không tìm thấy", "Hệ điều hành": "Không tìm thấy",
            "Kháng nước bụi": "Không tìm thấy", "URL": url, "Nguồn": self.source_name
        }

        try:
            name = self.driver.find_element(By.CSS_SELECTOR, ".box-product-name h1").text.strip()
            data["Tên sản phẩm"] = name
        except: pass

        try:
            price = self.driver.find_element(By.CSS_SELECTOR, ".sale-price").text.strip()
            data["Giá"] = price
        except: pass

        # Crawl specs
        try:
            btns = self.driver.find_elements(By.CSS_SELECTOR, "button.button__show-modal-technical")
            for b in btns:
                self.driver.execute_script("arguments[0].click();", b)
                time.sleep(1)
        except: pass

        rows = self.driver.find_elements(By.CSS_SELECTOR, "table.technical-content tr.technical-content-item")
        for row in rows:
            try:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 2: continue
                key = cols[0].text.lower().strip()
                val = cols[1].text.strip()
                if "công nghệ màn hình" in key: data["Công nghệ màn hình"] = val
                elif "camera sau" in key: data["Cam sau"] = val
                elif "camera trước" in key: data["Cam trước"] = val
                elif "chip" in key: data["Chip"] = val
                elif "sim" in key: data["Sim"] = val
                elif "ram" in key: data["RAM"] = val
                elif "bộ nhớ trong" in key: data["ROM"] = val
                elif "pin" in key: data["Pin"] = val
                elif "hệ điều hành" in key: data["Hệ điều hành"] = val
                elif "mạng" in key: data["Hỗ trợ mạng"] = val
                elif "ip" in key: data["Kháng nước bụi"] = val
            except: continue

        return data


# ----------------------- TGDD -----------------------
class TGDDCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("TheGioiDiDong")

    def get_all_product_links(self):
        print(f"[{self.source_name}] Lấy danh sách sản phẩm...")
        self.driver.get("https://www.thegioididong.com/dtdd")
        time.sleep(3)
        self.scroll_to_load_all_products()
        links = []
        try:
            elems = self.driver.find_elements(By.CSS_SELECTOR, "ul.listproduct a[href*='/dtdd/']")
            for el in elems:
                href = el.get_attribute("href")
                if href and '/dtdd/' in href and href not in links:
                    links.append(href.split('?')[0])
        except: pass
        print(f"✓ Tìm thấy {len(links)} sản phẩm")
        return links

    def scroll_to_load_all_products(self):
        for _ in range(20):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                btns = self.driver.find_elements(By.CSS_SELECTOR, ".view-more")
                for b in btns:
                    if b.is_displayed() and 'xem thêm' in b.text.lower():
                        self.driver.execute_script("arguments[0].click();", b)
                        time.sleep(2)
            except: break

    def crawl_single_product(self, url):
        try:
            self.driver.get(url)
            time.sleep(2)
            data = {
                "Tên sản phẩm": "Không tìm thấy", "Giá": "Không tìm thấy", "Công nghệ màn hình": "Không tìm thấy",
                "Cam sau": "Không tìm thấy", "Cam trước": "Không tìm thấy", "Chip": "Không tìm thấy",
                "Sim": "Không tìm thấy", "Hỗ trợ mạng": "Không tìm thấy", "RAM": "Không tìm thấy",
                "ROM": "Không tìm thấy", "Pin": "Không tìm thấy", "Hệ điều hành": "Không tìm thấy",
                "Kháng nước bụi": "Không tìm thấy", "URL": url, "Nguồn": self.source_name
            }
            try:
                name = self.driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
                data["Tên sản phẩm"] = name
            except: pass
            try:
                price = self.driver.find_element(By.CSS_SELECTOR, ".box-price-present").text.strip()
                data["Giá"] = price
            except: pass
            return data
        except:
            return None


# ----------------------- MANAGER -----------------------
class UnifiedCrawlerManager:
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
        self.crawlers = {
            'cellphones': CellphonesCrawler(),
            'tgdd': TGDDCrawler()
        }
        self.all_products = []

    def start_driver(self):
        opts = webdriver.ChromeOptions()
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        if self.headless: opts.add_argument("--headless")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        for c in self.crawlers.values():
            c.driver = self.driver
            c.wait = WebDriverWait(self.driver, 10)

    def close_driver(self):
        if self.driver: self.driver.quit()

    def crawl_source(self, source_name, max_products=None):
        crawler = self.crawlers[source_name]
        links = crawler.get_all_product_links()
        if max_products: links = links[:max_products]
        for url in links:
            data = crawler.crawl_single_product(url)
            if data: self.all_products.append(data)
        return len(links)

    def crawl_all_sources(self, sources=None, max_products=None):
        if sources is None: sources = list(self.crawlers.keys())
        for s in sources:
            self.crawl_source(s, max_products=max_products)

    def save_results(self, filename="unified_products2.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.all_products, f, ensure_ascii=False, indent=2)
        print(f"✓ Lưu kết quả {len(self.all_products)} sản phẩm vào {filename}")


# ----------------------- MAIN -----------------------
def main():
    HEADLESS = False
    MAX_PRODUCTS = 20
    SOURCES = ['cellphones', 'tgdd']

    manager = UnifiedCrawlerManager(headless=HEADLESS)
    manager.start_driver()
    try:
        manager.crawl_all_sources(sources=SOURCES, max_products=MAX_PRODUCTS)
        manager.save_results()
    finally:
        manager.close_driver()


if __name__ == "__main__":
    main()
