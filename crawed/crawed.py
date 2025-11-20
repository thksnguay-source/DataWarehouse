"""
CRAWL ĐƠN GIẢN - LẤY HẾT LINK TỪ TRANG MOBILE.HTML
==================================================
1. Lấy tất cả link sản phẩm từ mobile.html
2. Crawl từng sản phẩm một cách đơn giản
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import json


class SimpleCrawler:
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--window-size=1920,1080")
        self.chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        self.driver = None
        self.all_products = []

    def start_driver(self):
        """Khởi tạo trình duyệt"""
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.chrome_options
        )
        self.driver.maximize_window()

    def close_driver(self):
        """Đóng trình duyệt"""
        if self.driver:
            self.driver.quit()

    def get_all_product_links(self):
        """Lấy tất cả link sản phẩm từ trang mobile.html"""
        print("BƯỚC 1: LẤY TẤT CẢ LINK SẢN PHẨM TỪ MOBILE.HTML")
        print("=" * 60)

        # Vào trang mobile.html
        self.driver.get("https://cellphones.com.vn/mobile.html")
        time.sleep(3)
        print("✓ Đã vào trang mobile.html")

        product_links = []

        try:
            # Cuộn trang để load hết sản phẩm
            print("Đang cuộn trang để load hết sản phẩm...")
            self.scroll_to_load_all_products()

            # Tìm tất cả link sản phẩm
            print("Đang tìm tất cả link sản phẩm...")

            # Các pattern link sản phẩm
            link_patterns = [
                "//a[contains(@href, '/dien-thoai-')]",
                # "//a[contains(@href, '/iphone')]",
                # "//a[contains(@href, '/samsung')]",
                # "//a[contains(@href, '/xiaomi')]",
                # "//a[contains(@href, '/oppo')]",
                # "//a[contains(@href, '/tecno')]",
                # "//a[contains(@href, '/honor')]"
            ]

            for pattern in link_patterns:
                try:
                    elements = self.driver.find_elements(By.XPATH, pattern)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and self.is_valid_product_link(href):
                            if href not in product_links:
                                product_links.append(href)
                except Exception as e:
                    print(f"Lỗi khi tìm link với pattern {pattern}: {e}")
                    continue

            print(f"✓ Tìm thấy {len(product_links)} link sản phẩm")

            # In một vài link mẫu
            print("\nMột số link sản phẩm mẫu:")
            for i, link in enumerate(product_links[:5], 1):
                print(f"  {i}. {link}")

            return product_links

        except Exception as e:
            print(f"Lỗi khi lấy link sản phẩm: {e}")
            return []

    def is_valid_product_link(self, href):
        """Kiểm tra xem link có phải là link sản phẩm hợp lệ không"""
        if not href:
            return False

        # Các từ khóa cần có trong link sản phẩm
        required_keywords = ['dien-thoai', 'iphone', 'samsung', 'xiaomi', 'oppo', 'tecno', 'honor', 'nubia', 'sony',
                             'nokia']

        # Kiểm tra xem link có chứa ít nhất 1 từ khóa không
        return any(keyword in href.lower() for keyword in required_keywords)

    def scroll_to_load_all_products(self):
        """Cuộn trang để load hết tất cả sản phẩm"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_scroll_attempts = 10  # Giới hạn số lần cuộn

        while scroll_attempts < max_scroll_attempts:
            # Cuộn xuống cuối trang
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Tính chiều cao mới
            new_height = self.driver.execute_script("return document.body.scrollHeight")

            # Nếu không thay đổi chiều cao, dừng
            if new_height == last_height:
                break

            last_height = new_height
            scroll_attempts += 1
            print(f"  Đã cuộn {scroll_attempts} lần...")

    def crawl_single_product(self, product_url):
        """Crawl thông tin một sản phẩm"""
        try:
            self.driver.get(product_url)
            time.sleep(2)

            product_data = {
                "URL": product_url,
                "Nguồn": "CellphoneS"
            }

            # Lấy tên sản phẩm
            try:
                name_element = self.driver.find_element(By.CSS_SELECTOR, ".box-product-name h1")
                product_data["Tên sản phẩm"] = name_element.text.strip()
            except:
                product_data["Tên sản phẩm"] = "Không tìm thấy"

            # Lấy giá
            try:
                price_element = self.driver.find_element(By.CSS_SELECTOR, ".sale-price")
                product_data["Giá"] = price_element.text.strip()
            except:
                product_data["Giá"] = "Không tìm thấy"

            # Lấy thông số kỹ thuật
            try:
                # Click "Xem tất cả" nếu có
                try:
                    xem_them_btn = self.driver.find_element(By.CSS_SELECTOR, "button.button__show-modal-technical")
                    xem_them_btn.click()
                    time.sleep(1)
                except:
                    pass

                # Lấy bảng thông số
                rows = self.driver.find_elements(By.CSS_SELECTOR, "table.technical-content tr.technical-content-item")
                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) >= 2:
                        key = cols[0].text.strip().replace(":", "")
                        value = cols[1].text.strip().replace("\n", " ").replace("\r", " ")
                        if key and value:
                            product_data[key] = value
            except Exception as e:
                print(f"Lỗi khi lấy thông số sản phẩm {product_url}: {e}")

            return product_data

        except Exception as e:
            print(f"Lỗi khi crawl sản phẩm {product_url}: {e}")
            return None

    def crawl_all_products(self, product_links, max_products=None):
        """Crawl tất cả sản phẩm từ danh sách link"""
        print(f"\nBƯỚC 2: CRAWL {len(product_links)} SẢN PHẨM")
        print("=" * 60)

        if max_products:
            product_links = product_links[:max_products]
            print(f"Giới hạn crawl {max_products} sản phẩm đầu tiên")

        success_count = 0

        for i, product_url in enumerate(product_links, 1):
            print(f"Crawling sản phẩm {i}/{len(product_links)}: {product_url}")

            product_data = self.crawl_single_product(product_url)
            if product_data:
                self.all_products.append(product_data)
                success_count += 1
                print(f"  ✓ Thành công: {product_data.get('Tên sản phẩm', 'N/A')}")
            else:
                print(f"  ✗ Thất bại")

            # Nghỉ một chút để tránh bị block
            time.sleep(1)

        print(f"\n✓ Hoàn thành crawl: {success_count}/{len(product_links)} sản phẩm thành công")
        return success_count

    def save_results(self):
        """Lưu kết quả ra file"""
        # Lưu ra file JSON
        with open('cellphoneS.json', 'w', encoding='utf-8') as f:
            json.dump(self.all_products, f, ensure_ascii=False, indent=2)

        # In thống kê
        print(f"\n{'=' * 60}")
        print("KẾT QUẢ CRAWL ĐƠN GIẢN")
        print(f"{'=' * 60}")
        print(f"Tổng số sản phẩm: {len(self.all_products)}")

        # Thống kê theo thương hiệu (từ URL)
        brand_stats = {}
        for product in self.all_products:
            url = product.get('URL', '')
            if 'iphone' in url.lower():
                brand = 'Apple'
            elif 'samsung' in url.lower():
                brand = 'Samsung'
            elif 'xiaomi' in url.lower():
                brand = 'Xiaomi'
            elif 'oppo' in url.lower():
                brand = 'OPPO'
            elif 'tecno' in url.lower():
                brand = 'TECNO'
            elif 'honor' in url.lower():
                brand = 'HONOR'
            else:
                brand = 'Khác'

            brand_stats[brand] = brand_stats.get(brand, 0) + 1

        print("\nThống kê theo thương hiệu:")
        for brand, count in brand_stats.items():
            print(f"  {brand}: {count} sản phẩm")

        print(f"\n✓ Đã lưu kết quả vào file 'cellphoneS.json'")


def main():
    """Hàm chính"""
    crawler = SimpleCrawler()

    try:
        # Khởi tạo trình duyệt
        crawler.start_driver()

        # Bước 1: Lấy tất cả link sản phẩm
        product_links = crawler.get_all_product_links()

        if not product_links:
            print("Không tìm thấy link sản phẩm nào!")
            return

        # Bước 2: Crawl tất cả sản phẩm
        # Có thể giới hạn số lượng để test: max_products=10
        crawler.crawl_all_products(product_links, max_products=None)

        # Bước 3: Lưu kết quả
        crawler.save_results()

    except Exception as e:
        print(f"Lỗi chung: {e}")

    finally:
        # Đóng trình duyệt
        crawler.close_driver()


if __name__ == "__main__":
    main()