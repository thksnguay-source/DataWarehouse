from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import re


class SimpleCrawler:
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--window-size=1920,1080")
        self.chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        self.driver = None
        self.all_products = []

    # ======================================================
    # Khởi tạo / đóng trình duyệt
    # ======================================================
    def start_driver(self):
        """Khởi tạo trình duyệt"""
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=self.chrome_options
        )
        self.driver.maximize_window()

    def close_driver(self):
        """Đóng trình duyệt"""
        if self.driver:
            self.driver.quit()

    # ======================================================
    # Bước 1: Lấy link sản phẩm
    # ======================================================
    def get_all_product_links(self):
        """Lấy tất cả link sản phẩm từ trang mobile.html"""
        print("LẤY TẤT CẢ LINK SẢN PHẨM TỪ MOBILE.HTML")
        print("=" * 60)
        self.driver.get("https://cellphones.com.vn/mobile.html")

        WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.filter-sort__list-product"))
        )
        print("✓ Đã vào trang mobile.html")

        product_links = []

        try:
            print("Đang cuộn trang và nhấn 'Xem thêm'...")
            self.scroll_to_load_all_products()  # Cuộn và click max_clicks lần

            # Sau khi load xong mới lấy tất cả link sản phẩm
            container = self.driver.find_element(By.CSS_SELECTOR, "#blockFilterSort > div.filter-sort__list-product")
            elements = container.find_elements(By.CSS_SELECTOR, ".product-info > a")

            for element in elements:
                href = element.get_attribute("href")
                if href and self.is_valid_product_link(href):
                    if href not in product_links:
                        product_links.append(href)

            print(f"✓ Tìm thấy {len(product_links)} link sản phẩm hợp lệ")
            for i, link in enumerate(product_links[:1000], 1):
                print(f"  {i}. {link}")

            return product_links

        except Exception as e:
            print(f"Lỗi khi lấy link sản phẩm: {e}")
            return []

    def is_valid_product_link(self, href):
        """Kiểm tra xem link có phải là link sản phẩm hợp lệ không"""
        if not href:
            return False
        href = href.lower()
        invalid_patterns = ["bo-loc", "mobile/", "sforum", "tin-tuc"]
        if any(x in href for x in invalid_patterns):
            return False
        required_keywords = [
            "dien-thoai",
            "iphone",
            "samsung",
            "xiaomi",
            "oppo",
            "tecno",
            "honor",
            "nubia",
            "sony",
            "nokia",
            "vivo",
            "realme",
            "oneplus",
        ]
        return any(keyword in href.lower() for keyword in required_keywords)


    def scroll_to_load_all_products(self):

        max_clicks = 10  # số lần tối đa click
        wait_time = 10

        for i in range(max_clicks):
            try:
                # Cuộn xuống để nút hiện ra
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                show_more_btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#blockFilterSort > div.filter-sort__list-product > div > div.cps-block-content_btn-showmore > a"))
                )
                self.driver.execute_script("arguments[0].click();", show_more_btn)
                print(f"  ✅ Đã click 'Xem thêm' lần {i+1}")
                time.sleep(wait_time)
            except Exception:
                print(f"  ⛔ Dừng tại lần click thứ {i+1} (không còn nút hoặc load xong).")
                break

        print("✓ Đã load toàn bộ sản phẩm.")
    # ======================================================
    # Bước 2: Crawl chi tiết từng sản phẩm
    # ======================================================
    def crawl_single_product(self, product_url):
        """Crawl thông tin một sản phẩm - chỉ lấy các thông tin cần thiết"""
        try:
            self.driver.get(product_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".box-product-name h1")
                )
            )

            product_data = {
                "Tên sản phẩm": "Không tìm thấy",
                "Giá": "Không tìm thấy",
                "Công nghệ màn hình": "Không tìm thấy",
                "Cam sau": "Không tìm thấy",
                "Cam trước": "Không tìm thấy",
                "Chip": "Không tìm thấy",
                "Sim": "Không tìm thấy",
                "Hỗ trợ mạng": "Không tìm thấy",
                "Ram": "Không tìm thấy",
                "Rom": "Không tìm thấy",
                "Pin": "Không tìm thấy",
                "HDH": "Không tìm thấy",
                "Kháng nước bụi": "Không tìm thấy",
                "URL": product_url,
                "Nguồn": "CellphoneS",
            }

            # Tên sản phẩm
            try:
                product_data["Tên sản phẩm"] = self.driver.find_element(
                    By.CSS_SELECTOR, ".box-product-name h1"
                ).text.strip()
            except:
                pass

            # Giá
            try:
                product_data["Giá"] = self.driver.find_element(
                    By.CSS_SELECTOR, ".sale-price"
                ).text.strip()
            except:
                pass

            # Click "Xem tất cả" nếu có
            try:
                xem_them_btn = self.driver.find_element(
                    By.CSS_SELECTOR, "button.button__show-modal-technical"
                )
                self.driver.execute_script("arguments[0].click();", xem_them_btn)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table.technical-content")
                    )
                )
            except:
                pass

            # Lấy bảng thông số
            rows = self.driver.find_elements(
                By.CSS_SELECTOR, "table.technical-content tr.technical-content-item"
            )
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 2:
                    continue

                def clean_text(elem):
                    return (
                        " ".join(
                            elem.text.strip()
                            .replace(":", "")
                            .replace("\n", " ")
                            .replace("\r", " ")
                            .split()
                        )
                        .lower()
                    )

                key = clean_text(cols[0])
                value = clean_text(cols[1])

                # Xử lý mapping
                field_mappings = {
                    "Công nghệ màn hình": ["công nghệ màn hình"],
                    "Cam sau": ["camera sau"],
                    "Cam trước": ["camera trước"],
                    "Chip": ["chip", "chipset"],
                    "Sim": ["thẻ sim", "sim"],
                    "Ram": ["dung lượng ram"],
                    "Rom": ["bộ nhớ trong"],
                    "Pin": ["pin"],
                    "HDH": ["hệ điều hành"],
                }

                matched = False

                if "hỗ trợ mạng" in key or (
                    "mạng" in key and "màn" not in key and "nfc" not in key
                ):
                    product_data["Hỗ trợ mạng"] = value
                    matched = True
                elif "chỉ số kháng nước" in key or "kháng nước" in key:
                    product_data["Kháng nước bụi"] = value
                    matched = True
                elif any(ip in key for ip in ["ip68", "ip67", "ip65"]):
                    product_data["Kháng nước bụi"] = value
                    matched = True
                else:
                    for field, keywords in field_mappings.items():
                        if any(word in key for word in keywords):
                            product_data[field] = value
                            matched = True
                            break

                # if not matched:
                #     print(f"  [Unmatched] {key} = {value}")

            return product_data

        except Exception as e:
            print(f"Lỗi khi crawl sản phẩm {product_url}: {e}")
            return None

    # ======================================================
    # Bước 3: Crawl tất cả sản phẩm
    # ======================================================
    def crawl_all_products(self, product_links, max_products=None):
        print(f"\nBƯỚC 2: CRAWL {len(product_links)} SẢN PHẨM")
        print("=" * 60)
        if max_products:
            product_links = product_links[:max_products]
            print(f"Giới hạn crawl {max_products} sản phẩm đầu tiên")

        success_count = 0
        for i, link in enumerate(product_links, 1):
            print(f"\n[{i}/{len(product_links)}] Crawl: {link}")
            data = self.crawl_single_product(link)
            if data:
                self.all_products.append(data)
                success_count += 1
                print(f"  ✓ {data.get('Tên sản phẩm')}")
            else:
                print("  ✗ Thất bại")
            time.sleep(1)
        print(f"\n✓ Hoàn thành: {success_count}/{len(product_links)} sản phẩm")
        return success_count

    # ======================================================
    # Bước 4: Lưu kết quả
    # ======================================================
    def save_results(self):
        with open("simple_crawled_products.json", "w", encoding="utf-8") as f:
            json.dump(self.all_products, f, ensure_ascii=False, indent=2)

        print("\n" + "=" * 60)
        print("KẾT QUẢ CRAWL")
        print("=" * 60)
        print(f"Tổng sản phẩm: {len(self.all_products)}")

        # Thống kê thương hiệu
        brand_stats = {}
        for p in self.all_products:
            url = p.get("URL", "").lower()
            if "iphone" in url:
                brand = "Apple"
            elif "samsung" in url:
                brand = "Samsung"
            elif "xiaomi" in url:
                brand = "Xiaomi"
            elif "oppo" in url:
                brand = "OPPO"
            elif "tecno" in url:
                brand = "TECNO"
            elif "honor" in url:
                brand = "HONOR"
            else:
                brand = "Khác"
            brand_stats[brand] = brand_stats.get(brand, 0) + 1

        print("\nThống kê theo thương hiệu:")
        for brand, count in brand_stats.items():
            print(f"  {brand}: {count}")

        print("\n✓ Đã lưu kết quả vào file 'simple_crawled_products.json'")


# ======================================================
# HÀM CHÍNH
# ======================================================
def main():
    crawler = SimpleCrawler()
    try:
        crawler.start_driver()
        links = crawler.get_all_product_links()
        if not links:
            print("Không tìm thấy link sản phẩm!")
            return
        crawler.crawl_all_products(links, max_products=100) 
        crawler.save_results()
    except Exception as e:
        print(f"Lỗi chung: {e}")
    finally:
        crawler.close_driver()


if __name__ == "__main__":
    main()
