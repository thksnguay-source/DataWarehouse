-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Nov 18, 2025 at 01:54 PM
-- Server version: 10.4.32-MariaDB
-- PHP Version: 8.2.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `datawarehouse`
--

DELIMITER $$
--
-- Procedures
--
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_dw` (IN `p_batch_id` VARCHAR(255), IN `p_inserted` INT, IN `p_updated` INT, IN `p_skipped` INT)   BEGIN
    DECLARE v_etl_id INT;

    -- 1. Tạo batch log mới
    INSERT INTO etl_log (batch_id, source_table, target_table, status, records_inserted, records_updated, records_skipped)
    VALUES (p_batch_id, 'stg_products', 'dim_*', 'running', 0, 0, 0);

    SET v_etl_id = LAST_INSERT_ID();

    -- 2. Load dim_brand
    INSERT INTO dim_brand (brand_key, brand_name)
    SELECT DISTINCT brand_key, brand_name
    FROM stg_products
    WHERE brand_key NOT IN (SELECT brand_key FROM dim_brand);

    -- 3. Load dim_category
    INSERT INTO dim_category (category_key, category_name)
    SELECT DISTINCT category_key, category_name
    FROM stg_products
    WHERE category_key NOT IN (SELECT category_key FROM dim_category);

    -- 4. Load dim_source
    INSERT INTO dim_source (source_key, source_name)
    SELECT DISTINCT source_key, source_name
    FROM stg_products
    WHERE source_key NOT IN (SELECT source_key FROM dim_source);

    -- 5. Load dim_date
    INSERT INTO dim_date (date_key, date, year, month, day)
    SELECT DISTINCT date_key, date, year, month, day
    FROM stg_products
    WHERE date_key NOT IN (SELECT date_key FROM dim_date);

    -- 6. Load dim_product (SCD Type 1)
    REPLACE INTO dim_product
    SELECT * FROM stg_products;

    -- 7. Update etl_log
    UPDATE etl_log
    SET status = 'success',
        records_inserted = p_inserted,
        records_updated = p_updated,
        records_skipped = p_skipped,
        end_time = NOW()
    WHERE etl_id = v_etl_id;

END$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_update_etl_log` (IN `p_batch_id` VARCHAR(255), IN `p_records` INT, IN `p_status` VARCHAR(50))   BEGIN
    UPDATE etl_log
    SET 
        records_inserted = p_records,
        status = p_status,
        end_time = NOW()
    WHERE batch_id = p_batch_id;
END$$

DELIMITER ;

-- --------------------------------------------------------

--
-- Table structure for table `dim_brand`
--

CREATE TABLE `dim_brand` (
  `brand_key` bigint(20) DEFAULT NULL,
  `brand_name` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `dim_brand`
--

INSERT INTO `dim_brand` (`brand_key`, `brand_name`) VALUES
(1, 'OPPO'),
(2, 'HONOR'),
(3, 'TECNO'),
(4, 'Xiaomi'),
(5, 'Samsung'),
(6, 'Nothing'),
(7, 'Nubia'),
(8, 'Sony');

-- --------------------------------------------------------

--
-- Table structure for table `dim_category`
--

CREATE TABLE `dim_category` (
  `category_key` bigint(20) DEFAULT NULL,
  `category_name` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `dim_category`
--

INSERT INTO `dim_category` (`category_key`, `category_name`) VALUES
(1, 'Smartphone'),
(2, 'Foldable');

-- --------------------------------------------------------

--
-- Table structure for table `dim_date`
--

CREATE TABLE `dim_date` (
  `date_key` int(11) DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `year` int(11) DEFAULT NULL,
  `month` int(11) DEFAULT NULL,
  `day` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `dim_date`
--

INSERT INTO `dim_date` (`date_key`, `date`, `year`, `month`, `day`) VALUES
(20251029, '2025-10-29 01:56:47', 2025, 10, 29);

-- --------------------------------------------------------

--
-- Table structure for table `dim_product`
--

CREATE TABLE `dim_product` (
  `URL` text DEFAULT NULL,
  `Nguồn` text DEFAULT NULL,
  `Tên sản phẩm` text DEFAULT NULL,
  `Giá` text DEFAULT NULL,
  `Kích thước màn hình` text DEFAULT NULL,
  `Công nghệ màn hình` text DEFAULT NULL,
  `Độ phân giải màn hình` text DEFAULT NULL,
  `Tính năng màn hình` text DEFAULT NULL,
  `Tần số quét` text DEFAULT NULL,
  `Kiểu màn hình` text DEFAULT NULL,
  `Camera sau` text DEFAULT NULL,
  `Quay video` text DEFAULT NULL,
  `Tính năng camera` text DEFAULT NULL,
  `Camera trước` text DEFAULT NULL,
  `Chipset` text DEFAULT NULL,
  `GPU` text DEFAULT NULL,
  `Công nghệ NFC` text DEFAULT NULL,
  `Hỗ trợ mạng` text DEFAULT NULL,
  `GPS` text DEFAULT NULL,
  `Dung lượng RAM` text DEFAULT NULL,
  `Bộ nhớ trong` text DEFAULT NULL,
  `Pin` text DEFAULT NULL,
  `Công nghệ sạc` text DEFAULT NULL,
  `Cổng sạc` text DEFAULT NULL,
  `Hệ điều hành` text DEFAULT NULL,
  `Kích thước` text DEFAULT NULL,
  `Trọng lượng` text DEFAULT NULL,
  `Chỉ số kháng nước, bụi` text DEFAULT NULL,
  `Công nghệ âm thanh` text DEFAULT NULL,
  `Cảm biến vân tay` text DEFAULT NULL,
  `Các loại cảm biến` text DEFAULT NULL,
  `Tính năng đặc biệt` text DEFAULT NULL,
  `Wi-Fi` text DEFAULT NULL,
  `Bluetooth` text DEFAULT NULL,
  `Thẻ SIM` text DEFAULT NULL,
  `Loại CPU` text DEFAULT NULL,
  `Brand` text DEFAULT NULL,
  `Category` text DEFAULT NULL,
  `sale_price_vnd` double DEFAULT NULL,
  `Ngày_crawl` datetime DEFAULT NULL,
  `product_key` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `dim_product`
--

INSERT INTO `dim_product` (`URL`, `Nguồn`, `Tên sản phẩm`, `Giá`, `Kích thước màn hình`, `Công nghệ màn hình`, `Độ phân giải màn hình`, `Tính năng màn hình`, `Tần số quét`, `Kiểu màn hình`, `Camera sau`, `Quay video`, `Tính năng camera`, `Camera trước`, `Chipset`, `GPU`, `Công nghệ NFC`, `Hỗ trợ mạng`, `GPS`, `Dung lượng RAM`, `Bộ nhớ trong`, `Pin`, `Công nghệ sạc`, `Cổng sạc`, `Hệ điều hành`, `Kích thước`, `Trọng lượng`, `Chỉ số kháng nước, bụi`, `Công nghệ âm thanh`, `Cảm biến vân tay`, `Các loại cảm biến`, `Tính năng đặc biệt`, `Wi-Fi`, `Bluetooth`, `Thẻ SIM`, `Loại CPU`, `Brand`, `Category`, `sale_price_vnd`, `Ngày_crawl`, `product_key`) VALUES
('https://cellphones.com.vn/dien-thoai-oppo-find-x9.html', 'CellphoneS', 'OPPO Find X9 12GB 256GB', 'Liên hệ để báo giá', '6.59 inches', 'OLED', '1256 x 2760 pixels', 'Tần số quét: 120Hz Độ sáng tối đa: 3600 nits Màu sắc hiển thị: 1 tỷ màu', '120Hz', 'Đục lỗ (Nốt ruồi)', '50MP OIS (Chính) + 50MP OIS (Chân dung Tele) + 50MP (Góc rộng)', '4K', 'Camera 200MP tele zoom quang 3x, Cảm biến chính 50MP, Góc siêu rộng, Hỗ trợ AI chụp đêm, chân dung và chống rung OIS/EIS', '50MP', 'MediaTek Dimensity 9500', 'Mali-G1 Ultra MC12', 'Có', '5G', 'GPS, GLONASS, Galileo, Beidou', '12 GB', '256 GB', '7025mAh', 'Sạc nhanh có dây 80W Sạc không dây 50W', 'USB Type-C', 'ColorOS 16, nền tảng Android 16', '74.3 × 157.4 × 7.99 mm', '<210g', 'IP68/69', 'Loa kép', 'Cảm biến vân tay dưới màn hình', 'Cảm biến gia tốc, Cảm biến tiệm cận, Cảm biến ánh sáng, La bàn, Con quay hồi chuyển', 'Hỗ trợ 5G, Bảo mật vân tay, Nhận diện khuôn mặt, Kháng nước, kháng bụi, Điện thoại AI', 'Wi-Fi 6E (802.11 a/b/g/n/ac/ax)', '5.3', 'nan', 'nan', 'OPPO', 'Smartphone', NULL, '2025-10-29 01:56:47', 1),
('https://cellphones.com.vn/dien-thoai-honor-magic-v5.html', 'CellphoneS', 'HONOR Magic V5', '44.990.000đ', '6.43 inches', 'OLED', 'nan', 'Màn hình trong: 2352 x 2172 pixel Màn hình ngoài: 2376 x 1060 pixel 1,07 tỷ màu, Màn hình chăm sóc mắt cận thị AI, tần số 4320Hz PWM giảm nháy sáng, Điều chỉnh độ sáng động, Màn hình đêm theo nhịp sinh học, Màn hình tông màu tự nhiên', 'nan', 'nan', 'Camera góc siêu rộng 50MP (khẩu độ f/2.0) Camera góc rộng 50MP (khẩu độ f/1.6, OIS) Camera tele 64MP (khẩu độ f/2.5, OIS)', 'nan', 'nan', 'Màn hình trong: Camera góc rộng 20MP (f/2.2) Màn hình ngoài: Camera góc rộng 20MP (f/2.2)', 'Qualcomm Snapdragon 8 Elite', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '5820 mAh', 'nan', 'nan', 'MagicOS 9.0.1 (Dựa trên Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'SIM 1 + SIM 2 / SIM 1 + eSIM / 2 eSIM', 'Tám nhân (2×Prime 4,32 GHz+6×Performance 3,53 GHz)', 'HONOR', 'Foldable', 44990000, '2025-10-29 01:56:47', 2),
('https://cellphones.com.vn/dien-thoai-tecno-spark-40-pro-plus.html', 'CellphoneS', 'TECNO Spark 40 Pro+ 8GB 256GB (Chỉ có tại CellphoneS)', '5.190.000đ', '6.78 inches', 'AMOLED', 'nan', '1224 x 2720 pixels 6.78\'\' 144Hz & 1.5K 3D AMOLED Display', 'nan', 'nan', '50 MP', 'nan', 'nan', '13 MP', 'MediaTek Helio G200', 'nan', 'nan', 'nan', 'nan', '8 GB', '256 GB', '5200mAh, 45W', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'TECNO', 'Smartphone', 5190000, '2025-10-29 01:56:47', 3),
('https://cellphones.com.vn/dien-thoai-oppo-a6-pro.html', 'CellphoneS', 'OPPO A6 Pro 8GB 256GB', '7.990.000đ', '6.57 inches', 'AMOLED', '1080 x 2372 pixels (FullHD+)', '1 tỷ màu, Độ sáng tối đa 1400 nits', 'nan', 'nan', '50MP (Chính, f/1.8) + 2MP (Mono, f/2.4), có đèn flash', 'nan', 'nan', '16MP, f/2.4', 'MediaTek Helio G100, tối đa 2.2GHz', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '7000mAh', 'nan', 'nan', 'ColorOS 15 (Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân, tối đa 2.2GHz', 'OPPO', 'Smartphone', 7990000, '2025-10-29 01:56:47', 4),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-15c.html', 'CellphoneS', 'Xiaomi Redmi 15C 4GB 128GB NFC', '3.490.000đ', '6.9 inches', 'LCD', '1600 x 720 pixels (HD+)', 'Độ sáng 660 nits 810 nits (HBM) Tỉ lệ tương phản 1200:1 Cảm ứng 240Hz Đạt chứng nhận TÜV Rheinland: Giảm ánh sáng xanh, Không nhấp nháy, Thân thiện nhịp sinh học Hỗ trợ DC Dimming giảm nhấp nháy', 'nan', 'nan', '50MP, Ống kính -5P, f/1.8', 'nan', 'nan', '8MP f/2.0', 'MediaTek Helio G81-Ultra', 'nan', 'Có', 'nan', 'nan', '4 GB', '128 GB', '6000mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Octa-core (Cortex-A75 + Cortex-A55), tốc độ tối đa 2.0GHz', 'Xiaomi', 'Smartphone', 3490000, '2025-10-29 01:56:47', 5),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a17-5g.html', 'CellphoneS', 'Samsung Galaxy A17 5G 8GB 128GB', '5.890.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Độ sáng tối đa 800 nits Kính cường lực Corning Gorilla Glass 7', 'nan', 'nan', 'Chính 50 MP Phụ 5 MP, 2 MP', 'nan', 'nan', '13 MP', 'Exynos 1330', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân (2 nhân 2.4 GHz & 6 nhân 2.0 GHz)', 'Samsung', 'Smartphone', 5890000, '2025-10-29 01:56:47', 6),
('https://cellphones.com.vn/dien-thoai-nothing-phone-2a-plus.html', 'CellphoneS', 'Nothing Phone 2A Plus 5G 12GB 256GB - Chỉ có tại CellphoneS', '7.590.000đ', '6.7 inches', 'Flexible AMOLED On-cell', '1080 x 2412 pixels', '1300 nits 120 Hz, 1,07 tỷ màu', 'nan', 'nan', 'Camera chính: 50 MP, f/1.88, 1/1.57\" Camera góc siêu rộng: 50 MP, f/2.2, 1/2.76\"', 'nan', 'nan', '50 MP, f/2.2, 1/2.76\"', 'MediaTek Dimensity 7350 Pro 5G', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Up to 3.0 GHz', 'Nothing', 'Smartphone', 7590000, '2025-10-29 01:56:47', 7),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-13-pro-5g-8gb-256gb.html', 'CellphoneS', 'Xiaomi Redmi Note 13 Pro 5G 8GB 256GB', '6.450.000đ', '6.67 inches', 'AMOLED', '1220 x 2712 pixels', 'Tần số quét 120Hz, 1800 nits Kính cường lực Corning Gorilla Glass Victus', 'nan', 'nan', 'Chính 200 MP & Phụ 8 MP, 2 MP', 'nan', 'nan', '16 MP', 'Snapdragon 7s Gen 2 8 nhân', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5100 mAh', 'nan', 'nan', 'Android 13', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Dual nano-SIM hoặc 1 nano-SIM + 1 eSIM', '4 nhân 2.4 GHz & 4 nhân 1.95 GHz', 'Xiaomi', 'Smartphone', 6450000, '2025-10-29 01:56:47', 8),
('https://cellphones.com.vn/dien-thoai-tecno-camon-30s.html', 'CellphoneS', 'TECNO CAMON 30S 8GB 256GB', '3.990.000đ', '6.78 inches', 'AMOLED', '1080 x 2436 pixels', '120Hz Gorilla Glass 5', 'nan', 'nan', '50M OIS 2M + light sensor 100MP', 'nan', 'nan', '13MP', 'Mediatek G100', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5000mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'TECNO', 'Smartphone', 3990000, '2025-10-29 01:56:47', 9),
('https://cellphones.com.vn/dien-thoai-tecno-spark-30-pro-8gb-256gb.html', 'CellphoneS', 'TECNO SPARK 30 Pro 8GB 256GB Transformer - Chỉ có tại CellphoneS', '3.890.000đ', '6.78 inches', 'AMOLED', 'nan', '120Hz, 1700 nits', 'nan', 'nan', '108MP, f/1.75', 'nan', 'nan', '13MP', 'MediaTek Helio G100', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'TECNO', 'Smartphone', 3890000, '2025-10-29 01:56:47', 10),
('https://cellphones.com.vn/dien-thoai-honor-x9c-5g.html', 'CellphoneS', 'HONOR X9c 5G 12GB 256GB', '7.290.000đ', '6.78 inches', 'AMOLED', 'nan', '2700*1224 Pixels 1,07 tỷ màu 1.5 K độ phân giải 4000nit 100% DCI-P3', 'nan', 'nan', 'Camera chính 108MP (F1.75), OIS + Camera góc rộng 5MP (F2.2)', 'nan', 'nan', '16MP (F2.45)', 'Qualcomm Snapdragon 6 Gen 1', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6600mAh, 66W (120V/3.3A)', 'nan', 'nan', 'MagicOS 8.0, dựa trên Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', '4 x A78 *2.2GHz + 4 x A55*1.8GHz', 'HONOR', 'Smartphone', 7290000, '2025-10-29 01:56:47', 11),
('https://cellphones.com.vn/dien-thoai-xiaomi-15-12gb-512gb.html', 'CellphoneS', 'Xiaomi 15 5G 12GB 512GB', '20.990.000đ', '6.36 inches', 'CrystalRes AMOLED', '2670 x 1200 pixels', 'Độ sáng 3200 nits Tự động tinh chỉnh tần số quét 1-120Hz Tốc độ phản hồi cảm ứng lên đến 300Hz Hỗ trợ 68 tỷ màu Dải màu DCI-P3 Hỗ trợ Dolby Vision, HDR10+, HDR10 Độ sáng DC Công nghệ cảm ứng ướt', 'nan', 'nan', 'Camera chính Leica: 50MP, ƒ/1.62, OIS, Tiêu cự tương đương 23mm Camera tele Leica: 50MP, ƒ/2.0, OIS Camera góc siêu rộng Leica: 50MP, ƒ/2.2, 115°', 'nan', 'nan', 'Camera trước: 32MP, ƒ/2.0, 90°, Tiêu cự 21mm', 'Snapdragon 8 Elite (Tiến trình sản xuất 3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '512 GB', '5240 mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Sim kép (nano-Sim và e-Sim) - Hỗ trợ 2 e-Sim', '2 x Lõi Prime, tốc độ tối đa 4,32GHz 6 x Lõi hiệu suất, lên đến 3,53GHz', 'Xiaomi', 'Smartphone', 20990000, '2025-10-29 01:56:47', 12),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a06-5g.html', 'CellphoneS', 'Samsung Galaxy A06 5G 4GB 128GB', '3.120.000đ', '6.7 inches', 'PLS LCD', '720 x 1600 pixel', '16 triệu màu, 60Hz, 576 nits', 'nan', 'nan', '50.0 MP, f/1.8 2.0 MP, f/2.4', 'nan', 'nan', '8.0 MP, f/2.0', 'MediaTek Dimensity 6300', 'nan', 'nan', 'nan', 'nan', '4 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '2 nhân 2.0 GHz & 6 nhân 1.8 GHz', 'Samsung', 'Smartphone', 3120000, '2025-10-29 01:56:47', 13),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-14-pro-plus.html', 'CellphoneS', 'Xiaomi Redmi Note 14 Pro Plus 5G 8GB 256GB', '8.590.000đ', '6.67 inches', 'AMOLED', '2712 x 1220 pixels', 'Tần số quét: Lên đến 120Hz Độ sáng: 3000 nits Độ sâu màu: 12-bit Tỷ lệ tương phản: 5,000,000:1', 'nan', 'nan', 'Chính 200MP, OIS - f/1.65 Góc siêu rộng 8MP - f/2.2 Macro 2MP - f/2.4', 'nan', 'nan', 'Camera trước - f/2.2', 'Snapdragon® 7s Gen 3', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5110 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, xung nhịp 2.5Ghz', 'Xiaomi', 'Smartphone', 8590000, '2025-10-29 01:56:47', 14),
('https://cellphones.com.vn/dien-thoai-xiaomi-poco-x7-5g.html', 'CellphoneS', 'Xiaomi POCO X7 5G 12GB 512GB', '7.750.000đ', '6.67 inches', 'AMOLED', '2712 x 1220 pixels', 'Tần số quét: 120Hz, 500 nits, Độ sâu màu: 12bit, Tỷ lệ tương phản: 5.000.000:1, DCI-P3, Kính Corning® Gorilla® Victus® 2, Dolby Vision, HDR10+', 'nan', 'nan', 'Camera chính: 50MP, f/1.5 Camera góc siêu rộng: 8MP, f/2.2 Camera macro: 2MP, f/2.4', 'nan', 'nan', '20MP, f/2.2', 'Powerful Dimensity 7300-Ultra', 'nan', 'Có', 'nan', 'nan', '12 GB', '512 GB', '5110mAh', 'nan', 'nan', 'Xiaomi HyperOS', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'Octa-core CPU, up to 2.5GHz', 'Xiaomi', 'Smartphone', 7750000, '2025-10-29 01:56:47', 15),
('https://cellphones.com.vn/dien-thoai-tecno-pova-7-256gb.html', 'CellphoneS', 'Tecno Pova 7 8GB 256GB', '4.490.000đ', '6.78 inches', 'IPS LCD', '1080x2460 pixels', 'Tần số quét 120Hz, FHD+, Độ sáng tối đa 900nits', 'nan', 'nan', '108M+2M', 'nan', 'nan', '8MP', 'MediaTek Helio G100 Utimate', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '7000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2*A76 up to 2.2Ghz, 6*A55 up to 2.0Ghz', 'TECNO', 'Smartphone', 4490000, '2025-10-29 01:56:47', 16),
('https://cellphones.com.vn/dien-thoai-tecno-pova-7.html', 'CellphoneS', 'Tecno Pova 7 8GB 128GB', '3.990.000đ', '6.78 inches', 'IPS LCD', '1080x2460 pixels', 'Tần số quét 120Hz, FHD+, Độ sáng tối đa 900nits', 'nan', 'nan', '108M+2M', 'nan', 'nan', '8MP', 'MediaTek Helio G100 Utimate', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '7000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2*A76 up to 2.2Ghz, 6*A55 up to 2.0Ghz', 'TECNO', 'Smartphone', 3990000, '2025-10-29 01:56:47', 17),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a26.html', 'CellphoneS', 'Samsung Galaxy A26 5G 8GB 128GB', '6.270.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Tần số quét 120Hz, 800 nits, Mặt kính Gorilla Glass Victus', 'nan', 'nan', '50MP + 8MP + 2MP', 'nan', 'nan', '13MP', 'Exynos 1380 (Quartz)', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2.4GHz,2GHz, Octa-Core', 'Samsung', 'Smartphone', 6270000, '2025-10-29 01:56:47', 18),
('https://cellphones.com.vn/dien-thoai-poco-x7-pro-5g.html', 'CellphoneS', 'Xiaomi POCO X7 Pro 5G 12GB 256GB - Chỉ có tại CellphoneS', '9.090.000đ', '6.67 inches', 'AMOLED', '1220 x 2712 pixels', '3200 nits, Tần số quét 120Hz, Dolby Vison', 'nan', 'nan', '50MP', 'nan', 'nan', 'nan', 'Dimensity 8400-Ultra', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'Xiaomi', 'Smartphone', 9090000, '2025-10-29 01:56:47', 19),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a07.html', 'CellphoneS', 'Samsung Galaxy A07 4GB 128GB', '3.190.000đ', '6.7 inches', 'IPS LCD', '720 x 1600 pixel', 'Độ sáng tối đa 480 nits Kính cường lực Panda', 'nan', 'nan', 'Chính 50 MP Phụ 2 MP', 'nan', 'nan', '8 MP', 'MediaTek Helio G99', 'nan', 'Không', 'nan', 'nan', '4 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân (2 nhân 2.2 GHz & 6 nhân 2.0 GHz)', 'Samsung', 'Smartphone', 3190000, '2025-10-29 01:56:47', 20),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a56.html', 'CellphoneS', 'Samsung Galaxy A56 5G 8GB 128GB', '9.310.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', '16 triệu màu, 120 Hz, 1200 nit, kính cường lực Corning Gorilla Glass Victus+', 'nan', 'nan', '50.0 MP, F/1.8 + 12.0 MP, F/2.2 + 5.0 MP, F/2.4', 'nan', 'nan', '12.0 MP, F/2.2', 'Exynos 1580', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'SIM 1 + SIM 2 / SIM 1 + eSIM / 2 eSIM', '8 nhân 2.9GHz, 2.6GHz, 1.9GHz', 'Samsung', 'Smartphone', 9310000, '2025-10-29 01:56:47', 21),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a17.html', 'CellphoneS', 'Samsung Galaxy A17 8GB 128GB', '4.990.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Độ sáng tối đa 800 nits Kính cường lực Corning Gorilla Glass 7', 'nan', 'nan', 'Chính 50 MP Phụ 5 MP, 2 MP', 'nan', 'nan', '13 MP', 'MediaTek Helio G99', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân (2 nhân 2.2GHz & 6 nhân 2.0GHz)', 'Samsung', 'Smartphone', 4990000, '2025-10-29 01:56:47', 22),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-s25-ultra.html', 'CellphoneS', 'Samsung Galaxy S25 Ultra 12GB 256GB', '27.280.000đ', '6.9 inches', 'Dynamic AMOLED 2X', '3120 x 1440 pixels (Quad HD+)', '120Hz 2600 nits Corning® Gorilla® Armor 2', 'nan', 'nan', 'Camera siêu rộng 50MP Camera góc rộng 200 MP Camera Tele (5x) 50MP Camera Tele (3x) 10MP\"', 'nan', 'nan', '12 MP', 'Snapdragon 8 Elite dành cho Galaxy (3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM + eSIM', 'nan', 'Samsung', 'Smartphone', 27280000, '2025-10-29 01:56:47', 23),
('https://cellphones.com.vn/dien-thoai-xiaomi-15t-pro-5g.html', 'CellphoneS', 'Xiaomi 15T Pro 5G 12GB 512GB', '19.490.000đ', '6.83 inches', 'AMOLED', 'nan', 'Độ sáng tối đa 3200 nits, 447ppi, 68 tỷ màu, DCI-P3, HDR10+, Dolby Vision®, DC dimming, TÜV Rheinland', 'nan', 'nan', 'Chính: 50MP, f/1.62, OIS, cảm biến Light Fusion 900 Tele: 50MP, f/3.0, OIS, Samsung JN5, Zoom 5x, 10x lossless, Ultra Zoom 20x Siêu rộng: 12MP, f/2.2, 120° FOV', 'nan', 'nan', '32MP, f/2.2, tiêu cự 21mm', 'MediaTek Dimensity 9400+', 'nan', 'nan', 'nan', 'nan', '12 GB', '512 GB', '5500mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Xiaomi', 'Smartphone', 19490000, '2025-10-29 01:56:47', 24),
('https://cellphones.com.vn/dien-thoai-xiaomi-15t-5g.html', 'CellphoneS', 'Xiaomi 15T 5G 12GB 512GB', '14.990.000đ', '6.83 inches', 'AMOLED', 'nan', 'Độ sáng tối đa 3200 nits (25% vùng hiển thị) 68 tỷ màu DCI-P3 Chứng nhận TÜV Rheinland (Low Blue Light, Flicker Free, Circadian Friendly)', 'nan', 'nan', 'Chính: 50MP (23mm, f/1.7, OIS, Light Fusion 800) Tele: 50MP (46mm, f/1.9) Góc siêu rộng: 12MP (15mm, f/2.2, 120° FOV)', 'nan', 'nan', '32MP, f/2.2, tiêu cự 21mm', 'MediaTek Dimensity 8400-Ultra', 'nan', 'nan', 'nan', 'nan', '12 GB', '512 GB', '5500mAh', 'nan', 'nan', 'Xiaomi HyperOS 2, tích hợp Xiaomi HyperAI & Google Gemini', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Nano-SIM', 'nan', 'Xiaomi', 'Smartphone', 14990000, '2025-10-29 01:56:47', 25),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-z-flip-7.html', 'CellphoneS', 'Samsung Galaxy Z Flip7 12GB 256GB', '25.990.000đ', '6.9 inches', 'Dynamic AMOLED 2X', 'nan', 'Tần số quét: 120 Hz Độ phân giải màn hình chính: 2520 x 1080 (FHD+) Kích cỡ màn hình phụ: 4.1\" Độ phân giải màn hình phụ: 1048 x 948 Công nghệ màn hình phụ: Super AMOLED Độ sâu màu sắc: 16M', 'nan', 'nan', '50 MP, F1.8 + 12 MP, F2.2', 'nan', 'nan', '10MP, F2.2', 'nan', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4300mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '10 nhân, 3.3 GHz, 2.74GHz, 2.36GHz, 1.8GHz', 'Samsung', 'Foldable', 25990000, '2025-10-29 01:56:47', 26),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-z-fold-7.html', 'CellphoneS', 'Samsung Galaxy Z Fold7 12GB 256GB', '42.990.000đ', '8.0 inches', 'Dynamic AMOLED 2X', 'nan', 'Tần số quét: 120 Hz Độ phân giải màn hình chính: 2184 x 1968 (QXGA+) Kích cỡ màn hình phụ: 6.5\" Độ phân giải màn hình phụ: 2520 x 1080 (FHD+) Độ sâu màu sắc: 16M', 'nan', 'nan', '200 MP, F1.7 + 12 MP, F2.2 + 10 MP, F2.4', 'nan', 'nan', '10MP, F2.2', 'Snapdragon® 8 Elite 3nm for Galaxy', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4400mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, 4.47GHz, 3.5GHz', 'Samsung', 'Foldable', 42990000, '2025-10-29 01:56:47', 27),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-s25.html', 'CellphoneS', 'Samsung Galaxy S25 256GB', '16.690.000đ', '6.2 inches', 'Dynamic AMOLED 2X', '2340 x 1080-pixel', '120Hz 2600 nits Corning® Gorilla® Armor 2', 'nan', 'nan', 'Camera siêu rộng 12MP Camera góc rộng 50MP Camera Tele 10MP', 'nan', 'nan', '12MP', 'Snapdragon 8 Elite dành cho Galaxy (3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4000 mAh', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM hoặc 2 eSIM hoặc 1 Nano SIM + 1 eSIM', 'Tốc độ CPU 4.47GHz, 3.5GHz 8 nhân', 'Samsung', 'Smartphone', 16690000, '2025-10-29 01:56:47', 28),
('https://cellphones.com.vn/dien-thoai-oppo-find-n5.html', 'CellphoneS', 'OPPO FIND N5', '44.180.000đ', '8.12 inches', 'AMOLED', '2.480 x 2.248 pixels', 'Tần số: 120Hz Màn hình chính: Kính Pet+UTG, 1400nit Màn hình ngoài: 6.62 inch, 1140 x 2616 (FHD+), kính siêu mỏng Nanocrytal, 1600nit 1 tỷ màu sắc', 'nan', 'nan', '50MP, ƒ/1.8 OIS (Chính) + 8MP, ƒ/2.2 (Góc rộng) + 50MP, ƒ/2.7 (Tele)', 'nan', 'nan', 'Màn hình chính: 8MP, ƒ/2.4 Màn hình ngoài: 8MP, ƒ/2.4', 'Qualcomm Snapdragon® 8 Elite', 'nan', 'Có', 'nan', 'nan', '16 GB', '512 GB', '5600mAh', 'nan', 'nan', 'ColorOS 15, nền tảng Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Dual nano-SIM hoặc 1 nano-SIM + 1 eSIM', 'nan', 'OPPO', 'Smartphone', 44180000, '2025-10-29 01:56:47', 29),
('https://cellphones.com.vn/dien-thoai-nubia-neo-3-gt-12gb-256gb.html', 'CellphoneS', 'Nubia Neo 3 GT 12GB 256GB', '6.590.000đ', '6.8 inches', 'OLED', '1080 x 2392 pixels', 'FullHD+ Tần số 120Hz Độ sáng cực đại 1300 nit Gam màu DCI-P3 100% Tỷ lệ tương phản 1000000:1 Tần số lấy mẫu cảm ứng tức thời 1200Hz', 'nan', 'nan', '50MP + 2MP', 'nan', 'nan', '16MP', 'Unisoc T9100', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'Nubia', 'Smartphone', 6590000, '2025-10-29 01:56:47', 30),
('https://cellphones.com.vn/dien-thoai-oppo-reno14.html', 'CellphoneS', 'OPPO Reno14 5G 12GB 256GB', '15.700.000đ', '6.59 inches', 'AMOLED', '1256 x 2760 pixels', 'Tần số quét 120Hz, độ sáng 1200 nits, kính GG7i', 'nan', 'nan', 'Chính 50MP OIS, F/1.8 Tele 50MP OIS, F/2.8 Góc rộng 8MP, F/2.2', 'nan', 'nan', '50MP, F/2.0', 'MediaTek Dimensity 8350 5G, tối đa 3.35GHz', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000mAh', 'nan', 'nan', 'ColorOS 15, Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, 3.35GHz', 'OPPO', 'Smartphone', 15700000, '2025-10-29 01:56:47', 31),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-14-5g.html', 'CellphoneS', 'Xiaomi Redmi Note 14 5G 8GB 256GB', '6.790.000đ', '6.67 inches', 'AMOLED', '1080 x 2400 pixels (FullHD+)', 'Tần số quét 120Hz Độ sáng 2100 nits Độ sâu màu 8 bit Độ tương phản 5.000.000:1 Giải màu rộng DCI-P3 960Hz PWM dimming Corning Gorilla Glass 5', 'nan', 'nan', '108MP, ƒ/1.7 8MP góc siêu rộng, ƒ/2.2 2MP Macro, ƒ/2.4', 'nan', 'nan', '20MP, ƒ/2.2', 'MediaTek Dimensity 7025 - Ultra', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5110mAh', 'nan', 'nan', 'Xiaomi HyperOS', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Octa-core processor, up to 2.5GHz Tiền trình 6nm', 'Xiaomi', 'Smartphone', 6790000, '2025-10-29 01:56:47', 32),
('https://cellphones.com.vn/dien-thoai-sony-xperia-1-vii.html', 'CellphoneS', 'Sony Xperia 1 VII 12GB 256GB', '33.490.000đ', '6.5 inches', 'OLED', '1080 x 2340 pixels (FullHD+)', 'DCI-P3 100%, BT.2020, 10-bit, cảm biến ánh sáng trước/sau', 'nan', 'nan', 'Chính 52MP, f/1.9 Tele 12MP, f/2.3–f/3.5 Góc siêu rộng 50MP, f/2.0', 'nan', 'nan', '12MP, f/2.0', 'Snapdragon® 8 Elite', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5.000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Kryo', 'Sony', 'Smartphone', 33490000, '2025-10-29 01:56:47', 33),
('https://cellphones.com.vn/dien-thoai-nothing-phone-3a.html', 'CellphoneS', 'Nothing Phone 3A 8GB 128GB', '8.490.000đ', '6.77 inches', 'AMOLED', '1080 x 2392 pixels', '120Hz, NTSC 105%, 387 ppi, 3000nit, 1.07 tỷ màu, DCI-P3 100%, HDR10+', 'nan', 'nan', 'Camera chính: 50MP, F1.88 Camera góc siêu rộng: 8MP, F2.2 Camera Tele: 50MP, F2.0', 'nan', 'nan', '32MP, F2.2', 'Qualcomm SM7635 Snapdragon 7s Gen 3', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000mAh', 'nan', 'nan', 'Nothing OS 3.1 (Based on Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'Nothing', 'Smartphone', 8490000, '2025-10-29 01:56:47', 34);

-- --------------------------------------------------------

--
-- Table structure for table `dim_source`
--

CREATE TABLE `dim_source` (
  `source_key` bigint(20) DEFAULT NULL,
  `source_name` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `dim_source`
--

INSERT INTO `dim_source` (`source_key`, `source_name`) VALUES
(1, 'CellphoneS');

-- --------------------------------------------------------

--
-- Table structure for table `etl_log`
--

CREATE TABLE `etl_log` (
  `etl_id` int(11) NOT NULL,
  `batch_id` varchar(50) NOT NULL,
  `source_table` varchar(50) NOT NULL,
  `target_table` varchar(50) NOT NULL,
  `records_inserted` int(11) DEFAULT 0,
  `records_updated` int(11) DEFAULT 0,
  `records_skipped` int(11) DEFAULT 0,
  `status` enum('running','success','failed') DEFAULT 'running',
  `start_time` timestamp NOT NULL DEFAULT current_timestamp(),
  `end_time` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `etl_log`
--

INSERT INTO `etl_log` (`etl_id`, `batch_id`, `source_table`, `target_table`, `records_inserted`, `records_updated`, `records_skipped`, `status`, `start_time`, `end_time`) VALUES
(1, 'batch_20251029010459', 'stg_products', 'dim_*', 0, 0, 0, 'failed', '2025-10-28 18:04:59', '2025-10-28 18:04:59'),
(2, 'batch_20251029010620', 'stg_products', 'dim_*', 46, 0, 0, 'success', '2025-10-28 18:06:20', '2025-10-28 18:06:20'),
(3, 'batch_20251029011052', 'stg_products', 'dim_*', 46, 0, 0, 'success', '2025-10-28 18:10:52', '2025-10-28 18:10:52'),
(4, 'batch_20251029014627', 'stg_products', 'dim_*', 46, 0, 0, 'success', '2025-10-28 18:46:27', '2025-10-28 18:46:27'),
(5, 'batch_20251029015647', 'stg_products', 'dim_*', 34, 0, 0, 'success', '2025-10-28 18:56:47', '2025-10-28 18:56:47');

-- --------------------------------------------------------

--
-- Table structure for table `stg_products`
--

CREATE TABLE `stg_products` (
  `URL` text DEFAULT NULL,
  `Nguồn` text DEFAULT NULL,
  `Tên sản phẩm` text DEFAULT NULL,
  `Giá` text DEFAULT NULL,
  `Kích thước màn hình` text DEFAULT NULL,
  `Công nghệ màn hình` text DEFAULT NULL,
  `Độ phân giải màn hình` text DEFAULT NULL,
  `Tính năng màn hình` text DEFAULT NULL,
  `Tần số quét` text DEFAULT NULL,
  `Kiểu màn hình` text DEFAULT NULL,
  `Camera sau` text DEFAULT NULL,
  `Quay video` text DEFAULT NULL,
  `Tính năng camera` text DEFAULT NULL,
  `Camera trước` text DEFAULT NULL,
  `Chipset` text DEFAULT NULL,
  `GPU` text DEFAULT NULL,
  `Công nghệ NFC` text DEFAULT NULL,
  `Hỗ trợ mạng` text DEFAULT NULL,
  `GPS` text DEFAULT NULL,
  `Dung lượng RAM` text DEFAULT NULL,
  `Bộ nhớ trong` text DEFAULT NULL,
  `Pin` text DEFAULT NULL,
  `Công nghệ sạc` text DEFAULT NULL,
  `Cổng sạc` text DEFAULT NULL,
  `Hệ điều hành` text DEFAULT NULL,
  `Kích thước` text DEFAULT NULL,
  `Trọng lượng` text DEFAULT NULL,
  `Chỉ số kháng nước, bụi` text DEFAULT NULL,
  `Công nghệ âm thanh` text DEFAULT NULL,
  `Cảm biến vân tay` text DEFAULT NULL,
  `Các loại cảm biến` text DEFAULT NULL,
  `Tính năng đặc biệt` text DEFAULT NULL,
  `Wi-Fi` text DEFAULT NULL,
  `Bluetooth` text DEFAULT NULL,
  `Thẻ SIM` text DEFAULT NULL,
  `Loại CPU` text DEFAULT NULL,
  `Brand` text DEFAULT NULL,
  `Category` text DEFAULT NULL,
  `sale_price_vnd` double DEFAULT NULL,
  `Ngày_crawl` datetime DEFAULT NULL,
  `product_key` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `stg_products`
--

INSERT INTO `stg_products` (`URL`, `Nguồn`, `Tên sản phẩm`, `Giá`, `Kích thước màn hình`, `Công nghệ màn hình`, `Độ phân giải màn hình`, `Tính năng màn hình`, `Tần số quét`, `Kiểu màn hình`, `Camera sau`, `Quay video`, `Tính năng camera`, `Camera trước`, `Chipset`, `GPU`, `Công nghệ NFC`, `Hỗ trợ mạng`, `GPS`, `Dung lượng RAM`, `Bộ nhớ trong`, `Pin`, `Công nghệ sạc`, `Cổng sạc`, `Hệ điều hành`, `Kích thước`, `Trọng lượng`, `Chỉ số kháng nước, bụi`, `Công nghệ âm thanh`, `Cảm biến vân tay`, `Các loại cảm biến`, `Tính năng đặc biệt`, `Wi-Fi`, `Bluetooth`, `Thẻ SIM`, `Loại CPU`, `Brand`, `Category`, `sale_price_vnd`, `Ngày_crawl`, `product_key`) VALUES
('https://cellphones.com.vn/dien-thoai-oppo-find-x9.html', 'CellphoneS', 'OPPO Find X9 12GB 256GB', 'Liên hệ để báo giá', '6.59 inches', 'OLED', '1256 x 2760 pixels', 'Tần số quét: 120Hz Độ sáng tối đa: 3600 nits Màu sắc hiển thị: 1 tỷ màu', '120Hz', 'Đục lỗ (Nốt ruồi)', '50MP OIS (Chính) + 50MP OIS (Chân dung Tele) + 50MP (Góc rộng)', '4K', 'Camera 200MP tele zoom quang 3x, Cảm biến chính 50MP, Góc siêu rộng, Hỗ trợ AI chụp đêm, chân dung và chống rung OIS/EIS', '50MP', 'MediaTek Dimensity 9500', 'Mali-G1 Ultra MC12', 'Có', '5G', 'GPS, GLONASS, Galileo, Beidou', '12 GB', '256 GB', '7025mAh', 'Sạc nhanh có dây 80W Sạc không dây 50W', 'USB Type-C', 'ColorOS 16, nền tảng Android 16', '74.3 × 157.4 × 7.99 mm', '<210g', 'IP68/69', 'Loa kép', 'Cảm biến vân tay dưới màn hình', 'Cảm biến gia tốc, Cảm biến tiệm cận, Cảm biến ánh sáng, La bàn, Con quay hồi chuyển', 'Hỗ trợ 5G, Bảo mật vân tay, Nhận diện khuôn mặt, Kháng nước, kháng bụi, Điện thoại AI', 'Wi-Fi 6E (802.11 a/b/g/n/ac/ax)', '5.3', 'nan', 'nan', 'OPPO', 'Smartphone', NULL, '2025-10-29 01:56:47', 1),
('https://cellphones.com.vn/dien-thoai-honor-magic-v5.html', 'CellphoneS', 'HONOR Magic V5', '44.990.000đ', '6.43 inches', 'OLED', 'nan', 'Màn hình trong: 2352 x 2172 pixel Màn hình ngoài: 2376 x 1060 pixel 1,07 tỷ màu, Màn hình chăm sóc mắt cận thị AI, tần số 4320Hz PWM giảm nháy sáng, Điều chỉnh độ sáng động, Màn hình đêm theo nhịp sinh học, Màn hình tông màu tự nhiên', 'nan', 'nan', 'Camera góc siêu rộng 50MP (khẩu độ f/2.0) Camera góc rộng 50MP (khẩu độ f/1.6, OIS) Camera tele 64MP (khẩu độ f/2.5, OIS)', 'nan', 'nan', 'Màn hình trong: Camera góc rộng 20MP (f/2.2) Màn hình ngoài: Camera góc rộng 20MP (f/2.2)', 'Qualcomm Snapdragon 8 Elite', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '5820 mAh', 'nan', 'nan', 'MagicOS 9.0.1 (Dựa trên Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'SIM 1 + SIM 2 / SIM 1 + eSIM / 2 eSIM', 'Tám nhân (2×Prime 4,32 GHz+6×Performance 3,53 GHz)', 'HONOR', 'Foldable', 44990000, '2025-10-29 01:56:47', 2),
('https://cellphones.com.vn/dien-thoai-tecno-spark-40-pro-plus.html', 'CellphoneS', 'TECNO Spark 40 Pro+ 8GB 256GB (Chỉ có tại CellphoneS)', '5.190.000đ', '6.78 inches', 'AMOLED', 'nan', '1224 x 2720 pixels 6.78\'\' 144Hz & 1.5K 3D AMOLED Display', 'nan', 'nan', '50 MP', 'nan', 'nan', '13 MP', 'MediaTek Helio G200', 'nan', 'nan', 'nan', 'nan', '8 GB', '256 GB', '5200mAh, 45W', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'TECNO', 'Smartphone', 5190000, '2025-10-29 01:56:47', 3),
('https://cellphones.com.vn/dien-thoai-oppo-a6-pro.html', 'CellphoneS', 'OPPO A6 Pro 8GB 256GB', '7.990.000đ', '6.57 inches', 'AMOLED', '1080 x 2372 pixels (FullHD+)', '1 tỷ màu, Độ sáng tối đa 1400 nits', 'nan', 'nan', '50MP (Chính, f/1.8) + 2MP (Mono, f/2.4), có đèn flash', 'nan', 'nan', '16MP, f/2.4', 'MediaTek Helio G100, tối đa 2.2GHz', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '7000mAh', 'nan', 'nan', 'ColorOS 15 (Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân, tối đa 2.2GHz', 'OPPO', 'Smartphone', 7990000, '2025-10-29 01:56:47', 4),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-15c.html', 'CellphoneS', 'Xiaomi Redmi 15C 4GB 128GB NFC', '3.490.000đ', '6.9 inches', 'LCD', '1600 x 720 pixels (HD+)', 'Độ sáng 660 nits 810 nits (HBM) Tỉ lệ tương phản 1200:1 Cảm ứng 240Hz Đạt chứng nhận TÜV Rheinland: Giảm ánh sáng xanh, Không nhấp nháy, Thân thiện nhịp sinh học Hỗ trợ DC Dimming giảm nhấp nháy', 'nan', 'nan', '50MP, Ống kính -5P, f/1.8', 'nan', 'nan', '8MP f/2.0', 'MediaTek Helio G81-Ultra', 'nan', 'Có', 'nan', 'nan', '4 GB', '128 GB', '6000mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Octa-core (Cortex-A75 + Cortex-A55), tốc độ tối đa 2.0GHz', 'Xiaomi', 'Smartphone', 3490000, '2025-10-29 01:56:47', 5),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a17-5g.html', 'CellphoneS', 'Samsung Galaxy A17 5G 8GB 128GB', '5.890.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Độ sáng tối đa 800 nits Kính cường lực Corning Gorilla Glass 7', 'nan', 'nan', 'Chính 50 MP Phụ 5 MP, 2 MP', 'nan', 'nan', '13 MP', 'Exynos 1330', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân (2 nhân 2.4 GHz & 6 nhân 2.0 GHz)', 'Samsung', 'Smartphone', 5890000, '2025-10-29 01:56:47', 6),
('https://cellphones.com.vn/dien-thoai-nothing-phone-2a-plus.html', 'CellphoneS', 'Nothing Phone 2A Plus 5G 12GB 256GB - Chỉ có tại CellphoneS', '7.590.000đ', '6.7 inches', 'Flexible AMOLED On-cell', '1080 x 2412 pixels', '1300 nits 120 Hz, 1,07 tỷ màu', 'nan', 'nan', 'Camera chính: 50 MP, f/1.88, 1/1.57\" Camera góc siêu rộng: 50 MP, f/2.2, 1/2.76\"', 'nan', 'nan', '50 MP, f/2.2, 1/2.76\"', 'MediaTek Dimensity 7350 Pro 5G', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Up to 3.0 GHz', 'Nothing', 'Smartphone', 7590000, '2025-10-29 01:56:47', 7),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-13-pro-5g-8gb-256gb.html', 'CellphoneS', 'Xiaomi Redmi Note 13 Pro 5G 8GB 256GB', '6.450.000đ', '6.67 inches', 'AMOLED', '1220 x 2712 pixels', 'Tần số quét 120Hz, 1800 nits Kính cường lực Corning Gorilla Glass Victus', 'nan', 'nan', 'Chính 200 MP & Phụ 8 MP, 2 MP', 'nan', 'nan', '16 MP', 'Snapdragon 7s Gen 2 8 nhân', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5100 mAh', 'nan', 'nan', 'Android 13', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Dual nano-SIM hoặc 1 nano-SIM + 1 eSIM', '4 nhân 2.4 GHz & 4 nhân 1.95 GHz', 'Xiaomi', 'Smartphone', 6450000, '2025-10-29 01:56:47', 8),
('https://cellphones.com.vn/dien-thoai-tecno-camon-30s.html', 'CellphoneS', 'TECNO CAMON 30S 8GB 256GB', '3.990.000đ', '6.78 inches', 'AMOLED', '1080 x 2436 pixels', '120Hz Gorilla Glass 5', 'nan', 'nan', '50M OIS 2M + light sensor 100MP', 'nan', 'nan', '13MP', 'Mediatek G100', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5000mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'TECNO', 'Smartphone', 3990000, '2025-10-29 01:56:47', 9),
('https://cellphones.com.vn/dien-thoai-tecno-spark-30-pro-8gb-256gb.html', 'CellphoneS', 'TECNO SPARK 30 Pro 8GB 256GB Transformer - Chỉ có tại CellphoneS', '3.890.000đ', '6.78 inches', 'AMOLED', 'nan', '120Hz, 1700 nits', 'nan', 'nan', '108MP, f/1.75', 'nan', 'nan', '13MP', 'MediaTek Helio G100', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'TECNO', 'Smartphone', 3890000, '2025-10-29 01:56:47', 10),
('https://cellphones.com.vn/dien-thoai-honor-x9c-5g.html', 'CellphoneS', 'HONOR X9c 5G 12GB 256GB', '7.290.000đ', '6.78 inches', 'AMOLED', 'nan', '2700*1224 Pixels 1,07 tỷ màu 1.5 K độ phân giải 4000nit 100% DCI-P3', 'nan', 'nan', 'Camera chính 108MP (F1.75), OIS + Camera góc rộng 5MP (F2.2)', 'nan', 'nan', '16MP (F2.45)', 'Qualcomm Snapdragon 6 Gen 1', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6600mAh, 66W (120V/3.3A)', 'nan', 'nan', 'MagicOS 8.0, dựa trên Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', '4 x A78 *2.2GHz + 4 x A55*1.8GHz', 'HONOR', 'Smartphone', 7290000, '2025-10-29 01:56:47', 11),
('https://cellphones.com.vn/dien-thoai-xiaomi-15-12gb-512gb.html', 'CellphoneS', 'Xiaomi 15 5G 12GB 512GB', '20.990.000đ', '6.36 inches', 'CrystalRes AMOLED', '2670 x 1200 pixels', 'Độ sáng 3200 nits Tự động tinh chỉnh tần số quét 1-120Hz Tốc độ phản hồi cảm ứng lên đến 300Hz Hỗ trợ 68 tỷ màu Dải màu DCI-P3 Hỗ trợ Dolby Vision, HDR10+, HDR10 Độ sáng DC Công nghệ cảm ứng ướt', 'nan', 'nan', 'Camera chính Leica: 50MP, ƒ/1.62, OIS, Tiêu cự tương đương 23mm Camera tele Leica: 50MP, ƒ/2.0, OIS Camera góc siêu rộng Leica: 50MP, ƒ/2.2, 115°', 'nan', 'nan', 'Camera trước: 32MP, ƒ/2.0, 90°, Tiêu cự 21mm', 'Snapdragon 8 Elite (Tiến trình sản xuất 3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '512 GB', '5240 mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Sim kép (nano-Sim và e-Sim) - Hỗ trợ 2 e-Sim', '2 x Lõi Prime, tốc độ tối đa 4,32GHz 6 x Lõi hiệu suất, lên đến 3,53GHz', 'Xiaomi', 'Smartphone', 20990000, '2025-10-29 01:56:47', 12),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a06-5g.html', 'CellphoneS', 'Samsung Galaxy A06 5G 4GB 128GB', '3.120.000đ', '6.7 inches', 'PLS LCD', '720 x 1600 pixel', '16 triệu màu, 60Hz, 576 nits', 'nan', 'nan', '50.0 MP, f/1.8 2.0 MP, f/2.4', 'nan', 'nan', '8.0 MP, f/2.0', 'MediaTek Dimensity 6300', 'nan', 'nan', 'nan', 'nan', '4 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '2 nhân 2.0 GHz & 6 nhân 1.8 GHz', 'Samsung', 'Smartphone', 3120000, '2025-10-29 01:56:47', 13),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-14-pro-plus.html', 'CellphoneS', 'Xiaomi Redmi Note 14 Pro Plus 5G 8GB 256GB', '8.590.000đ', '6.67 inches', 'AMOLED', '2712 x 1220 pixels', 'Tần số quét: Lên đến 120Hz Độ sáng: 3000 nits Độ sâu màu: 12-bit Tỷ lệ tương phản: 5,000,000:1', 'nan', 'nan', 'Chính 200MP, OIS - f/1.65 Góc siêu rộng 8MP - f/2.2 Macro 2MP - f/2.4', 'nan', 'nan', 'Camera trước - f/2.2', 'Snapdragon® 7s Gen 3', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5110 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, xung nhịp 2.5Ghz', 'Xiaomi', 'Smartphone', 8590000, '2025-10-29 01:56:47', 14),
('https://cellphones.com.vn/dien-thoai-xiaomi-poco-x7-5g.html', 'CellphoneS', 'Xiaomi POCO X7 5G 12GB 512GB', '7.750.000đ', '6.67 inches', 'AMOLED', '2712 x 1220 pixels', 'Tần số quét: 120Hz, 500 nits, Độ sâu màu: 12bit, Tỷ lệ tương phản: 5.000.000:1, DCI-P3, Kính Corning® Gorilla® Victus® 2, Dolby Vision, HDR10+', 'nan', 'nan', 'Camera chính: 50MP, f/1.5 Camera góc siêu rộng: 8MP, f/2.2 Camera macro: 2MP, f/2.4', 'nan', 'nan', '20MP, f/2.2', 'Powerful Dimensity 7300-Ultra', 'nan', 'Có', 'nan', 'nan', '12 GB', '512 GB', '5110mAh', 'nan', 'nan', 'Xiaomi HyperOS', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'Octa-core CPU, up to 2.5GHz', 'Xiaomi', 'Smartphone', 7750000, '2025-10-29 01:56:47', 15),
('https://cellphones.com.vn/dien-thoai-tecno-pova-7-256gb.html', 'CellphoneS', 'Tecno Pova 7 8GB 256GB', '4.490.000đ', '6.78 inches', 'IPS LCD', '1080x2460 pixels', 'Tần số quét 120Hz, FHD+, Độ sáng tối đa 900nits', 'nan', 'nan', '108M+2M', 'nan', 'nan', '8MP', 'MediaTek Helio G100 Utimate', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '7000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2*A76 up to 2.2Ghz, 6*A55 up to 2.0Ghz', 'TECNO', 'Smartphone', 4490000, '2025-10-29 01:56:47', 16),
('https://cellphones.com.vn/dien-thoai-tecno-pova-7.html', 'CellphoneS', 'Tecno Pova 7 8GB 128GB', '3.990.000đ', '6.78 inches', 'IPS LCD', '1080x2460 pixels', 'Tần số quét 120Hz, FHD+, Độ sáng tối đa 900nits', 'nan', 'nan', '108M+2M', 'nan', 'nan', '8MP', 'MediaTek Helio G100 Utimate', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '7000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2*A76 up to 2.2Ghz, 6*A55 up to 2.0Ghz', 'TECNO', 'Smartphone', 3990000, '2025-10-29 01:56:47', 17),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a26.html', 'CellphoneS', 'Samsung Galaxy A26 5G 8GB 128GB', '6.270.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Tần số quét 120Hz, 800 nits, Mặt kính Gorilla Glass Victus', 'nan', 'nan', '50MP + 8MP + 2MP', 'nan', 'nan', '13MP', 'Exynos 1380 (Quartz)', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2.4GHz,2GHz, Octa-Core', 'Samsung', 'Smartphone', 6270000, '2025-10-29 01:56:47', 18),
('https://cellphones.com.vn/dien-thoai-poco-x7-pro-5g.html', 'CellphoneS', 'Xiaomi POCO X7 Pro 5G 12GB 256GB - Chỉ có tại CellphoneS', '9.090.000đ', '6.67 inches', 'AMOLED', '1220 x 2712 pixels', '3200 nits, Tần số quét 120Hz, Dolby Vison', 'nan', 'nan', '50MP', 'nan', 'nan', 'nan', 'Dimensity 8400-Ultra', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'Xiaomi', 'Smartphone', 9090000, '2025-10-29 01:56:47', 19),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a07.html', 'CellphoneS', 'Samsung Galaxy A07 4GB 128GB', '3.190.000đ', '6.7 inches', 'IPS LCD', '720 x 1600 pixel', 'Độ sáng tối đa 480 nits Kính cường lực Panda', 'nan', 'nan', 'Chính 50 MP Phụ 2 MP', 'nan', 'nan', '8 MP', 'MediaTek Helio G99', 'nan', 'Không', 'nan', 'nan', '4 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân (2 nhân 2.2 GHz & 6 nhân 2.0 GHz)', 'Samsung', 'Smartphone', 3190000, '2025-10-29 01:56:47', 20),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a56.html', 'CellphoneS', 'Samsung Galaxy A56 5G 8GB 128GB', '9.310.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', '16 triệu màu, 120 Hz, 1200 nit, kính cường lực Corning Gorilla Glass Victus+', 'nan', 'nan', '50.0 MP, F/1.8 + 12.0 MP, F/2.2 + 5.0 MP, F/2.4', 'nan', 'nan', '12.0 MP, F/2.2', 'Exynos 1580', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'SIM 1 + SIM 2 / SIM 1 + eSIM / 2 eSIM', '8 nhân 2.9GHz, 2.6GHz, 1.9GHz', 'Samsung', 'Smartphone', 9310000, '2025-10-29 01:56:47', 21),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a17.html', 'CellphoneS', 'Samsung Galaxy A17 8GB 128GB', '4.990.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Độ sáng tối đa 800 nits Kính cường lực Corning Gorilla Glass 7', 'nan', 'nan', 'Chính 50 MP Phụ 5 MP, 2 MP', 'nan', 'nan', '13 MP', 'MediaTek Helio G99', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân (2 nhân 2.2GHz & 6 nhân 2.0GHz)', 'Samsung', 'Smartphone', 4990000, '2025-10-29 01:56:47', 22),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-s25-ultra.html', 'CellphoneS', 'Samsung Galaxy S25 Ultra 12GB 256GB', '27.280.000đ', '6.9 inches', 'Dynamic AMOLED 2X', '3120 x 1440 pixels (Quad HD+)', '120Hz 2600 nits Corning® Gorilla® Armor 2', 'nan', 'nan', 'Camera siêu rộng 50MP Camera góc rộng 200 MP Camera Tele (5x) 50MP Camera Tele (3x) 10MP\"', 'nan', 'nan', '12 MP', 'Snapdragon 8 Elite dành cho Galaxy (3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM + eSIM', 'nan', 'Samsung', 'Smartphone', 27280000, '2025-10-29 01:56:47', 23),
('https://cellphones.com.vn/dien-thoai-xiaomi-15t-pro-5g.html', 'CellphoneS', 'Xiaomi 15T Pro 5G 12GB 512GB', '19.490.000đ', '6.83 inches', 'AMOLED', 'nan', 'Độ sáng tối đa 3200 nits, 447ppi, 68 tỷ màu, DCI-P3, HDR10+, Dolby Vision®, DC dimming, TÜV Rheinland', 'nan', 'nan', 'Chính: 50MP, f/1.62, OIS, cảm biến Light Fusion 900 Tele: 50MP, f/3.0, OIS, Samsung JN5, Zoom 5x, 10x lossless, Ultra Zoom 20x Siêu rộng: 12MP, f/2.2, 120° FOV', 'nan', 'nan', '32MP, f/2.2, tiêu cự 21mm', 'MediaTek Dimensity 9400+', 'nan', 'nan', 'nan', 'nan', '12 GB', '512 GB', '5500mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Xiaomi', 'Smartphone', 19490000, '2025-10-29 01:56:47', 24),
('https://cellphones.com.vn/dien-thoai-xiaomi-15t-5g.html', 'CellphoneS', 'Xiaomi 15T 5G 12GB 512GB', '14.990.000đ', '6.83 inches', 'AMOLED', 'nan', 'Độ sáng tối đa 3200 nits (25% vùng hiển thị) 68 tỷ màu DCI-P3 Chứng nhận TÜV Rheinland (Low Blue Light, Flicker Free, Circadian Friendly)', 'nan', 'nan', 'Chính: 50MP (23mm, f/1.7, OIS, Light Fusion 800) Tele: 50MP (46mm, f/1.9) Góc siêu rộng: 12MP (15mm, f/2.2, 120° FOV)', 'nan', 'nan', '32MP, f/2.2, tiêu cự 21mm', 'MediaTek Dimensity 8400-Ultra', 'nan', 'nan', 'nan', 'nan', '12 GB', '512 GB', '5500mAh', 'nan', 'nan', 'Xiaomi HyperOS 2, tích hợp Xiaomi HyperAI & Google Gemini', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Nano-SIM', 'nan', 'Xiaomi', 'Smartphone', 14990000, '2025-10-29 01:56:47', 25),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-z-flip-7.html', 'CellphoneS', 'Samsung Galaxy Z Flip7 12GB 256GB', '25.990.000đ', '6.9 inches', 'Dynamic AMOLED 2X', 'nan', 'Tần số quét: 120 Hz Độ phân giải màn hình chính: 2520 x 1080 (FHD+) Kích cỡ màn hình phụ: 4.1\" Độ phân giải màn hình phụ: 1048 x 948 Công nghệ màn hình phụ: Super AMOLED Độ sâu màu sắc: 16M', 'nan', 'nan', '50 MP, F1.8 + 12 MP, F2.2', 'nan', 'nan', '10MP, F2.2', 'nan', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4300mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '10 nhân, 3.3 GHz, 2.74GHz, 2.36GHz, 1.8GHz', 'Samsung', 'Foldable', 25990000, '2025-10-29 01:56:47', 26),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-z-fold-7.html', 'CellphoneS', 'Samsung Galaxy Z Fold7 12GB 256GB', '42.990.000đ', '8.0 inches', 'Dynamic AMOLED 2X', 'nan', 'Tần số quét: 120 Hz Độ phân giải màn hình chính: 2184 x 1968 (QXGA+) Kích cỡ màn hình phụ: 6.5\" Độ phân giải màn hình phụ: 2520 x 1080 (FHD+) Độ sâu màu sắc: 16M', 'nan', 'nan', '200 MP, F1.7 + 12 MP, F2.2 + 10 MP, F2.4', 'nan', 'nan', '10MP, F2.2', 'Snapdragon® 8 Elite 3nm for Galaxy', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4400mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, 4.47GHz, 3.5GHz', 'Samsung', 'Foldable', 42990000, '2025-10-29 01:56:47', 27),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-s25.html', 'CellphoneS', 'Samsung Galaxy S25 256GB', '16.690.000đ', '6.2 inches', 'Dynamic AMOLED 2X', '2340 x 1080-pixel', '120Hz 2600 nits Corning® Gorilla® Armor 2', 'nan', 'nan', 'Camera siêu rộng 12MP Camera góc rộng 50MP Camera Tele 10MP', 'nan', 'nan', '12MP', 'Snapdragon 8 Elite dành cho Galaxy (3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4000 mAh', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM hoặc 2 eSIM hoặc 1 Nano SIM + 1 eSIM', 'Tốc độ CPU 4.47GHz, 3.5GHz 8 nhân', 'Samsung', 'Smartphone', 16690000, '2025-10-29 01:56:47', 28),
('https://cellphones.com.vn/dien-thoai-oppo-find-n5.html', 'CellphoneS', 'OPPO FIND N5', '44.180.000đ', '8.12 inches', 'AMOLED', '2.480 x 2.248 pixels', 'Tần số: 120Hz Màn hình chính: Kính Pet+UTG, 1400nit Màn hình ngoài: 6.62 inch, 1140 x 2616 (FHD+), kính siêu mỏng Nanocrytal, 1600nit 1 tỷ màu sắc', 'nan', 'nan', '50MP, ƒ/1.8 OIS (Chính) + 8MP, ƒ/2.2 (Góc rộng) + 50MP, ƒ/2.7 (Tele)', 'nan', 'nan', 'Màn hình chính: 8MP, ƒ/2.4 Màn hình ngoài: 8MP, ƒ/2.4', 'Qualcomm Snapdragon® 8 Elite', 'nan', 'Có', 'nan', 'nan', '16 GB', '512 GB', '5600mAh', 'nan', 'nan', 'ColorOS 15, nền tảng Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Dual nano-SIM hoặc 1 nano-SIM + 1 eSIM', 'nan', 'OPPO', 'Smartphone', 44180000, '2025-10-29 01:56:47', 29),
('https://cellphones.com.vn/dien-thoai-nubia-neo-3-gt-12gb-256gb.html', 'CellphoneS', 'Nubia Neo 3 GT 12GB 256GB', '6.590.000đ', '6.8 inches', 'OLED', '1080 x 2392 pixels', 'FullHD+ Tần số 120Hz Độ sáng cực đại 1300 nit Gam màu DCI-P3 100% Tỷ lệ tương phản 1000000:1 Tần số lấy mẫu cảm ứng tức thời 1200Hz', 'nan', 'nan', '50MP + 2MP', 'nan', 'nan', '16MP', 'Unisoc T9100', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'Nubia', 'Smartphone', 6590000, '2025-10-29 01:56:47', 30),
('https://cellphones.com.vn/dien-thoai-oppo-reno14.html', 'CellphoneS', 'OPPO Reno14 5G 12GB 256GB', '15.700.000đ', '6.59 inches', 'AMOLED', '1256 x 2760 pixels', 'Tần số quét 120Hz, độ sáng 1200 nits, kính GG7i', 'nan', 'nan', 'Chính 50MP OIS, F/1.8 Tele 50MP OIS, F/2.8 Góc rộng 8MP, F/2.2', 'nan', 'nan', '50MP, F/2.0', 'MediaTek Dimensity 8350 5G, tối đa 3.35GHz', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000mAh', 'nan', 'nan', 'ColorOS 15, Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, 3.35GHz', 'OPPO', 'Smartphone', 15700000, '2025-10-29 01:56:47', 31),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-14-5g.html', 'CellphoneS', 'Xiaomi Redmi Note 14 5G 8GB 256GB', '6.790.000đ', '6.67 inches', 'AMOLED', '1080 x 2400 pixels (FullHD+)', 'Tần số quét 120Hz Độ sáng 2100 nits Độ sâu màu 8 bit Độ tương phản 5.000.000:1 Giải màu rộng DCI-P3 960Hz PWM dimming Corning Gorilla Glass 5', 'nan', 'nan', '108MP, ƒ/1.7 8MP góc siêu rộng, ƒ/2.2 2MP Macro, ƒ/2.4', 'nan', 'nan', '20MP, ƒ/2.2', 'MediaTek Dimensity 7025 - Ultra', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5110mAh', 'nan', 'nan', 'Xiaomi HyperOS', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Octa-core processor, up to 2.5GHz Tiền trình 6nm', 'Xiaomi', 'Smartphone', 6790000, '2025-10-29 01:56:47', 32),
('https://cellphones.com.vn/dien-thoai-sony-xperia-1-vii.html', 'CellphoneS', 'Sony Xperia 1 VII 12GB 256GB', '33.490.000đ', '6.5 inches', 'OLED', '1080 x 2340 pixels (FullHD+)', 'DCI-P3 100%, BT.2020, 10-bit, cảm biến ánh sáng trước/sau', 'nan', 'nan', 'Chính 52MP, f/1.9 Tele 12MP, f/2.3–f/3.5 Góc siêu rộng 50MP, f/2.0', 'nan', 'nan', '12MP, f/2.0', 'Snapdragon® 8 Elite', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5.000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Kryo', 'Sony', 'Smartphone', 33490000, '2025-10-29 01:56:47', 33),
('https://cellphones.com.vn/dien-thoai-nothing-phone-3a.html', 'CellphoneS', 'Nothing Phone 3A 8GB 128GB', '8.490.000đ', '6.77 inches', 'AMOLED', '1080 x 2392 pixels', '120Hz, NTSC 105%, 387 ppi, 3000nit, 1.07 tỷ màu, DCI-P3 100%, HDR10+', 'nan', 'nan', 'Camera chính: 50MP, F1.88 Camera góc siêu rộng: 8MP, F2.2 Camera Tele: 50MP, F2.0', 'nan', 'nan', '32MP, F2.2', 'Qualcomm SM7635 Snapdragon 7s Gen 3', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000mAh', 'nan', 'nan', 'Nothing OS 3.1 (Based on Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'Nothing', 'Smartphone', 8490000, '2025-10-29 01:56:47', 34),
('https://cellphones.com.vn/dien-thoai-oppo-find-x9.html', 'CellphoneS', 'OPPO Find X9 12GB 256GB', 'Liên hệ để báo giá', '6.59 inches', 'OLED', '1256 x 2760 pixels', 'Tần số quét: 120Hz Độ sáng tối đa: 3600 nits Màu sắc hiển thị: 1 tỷ màu', '120Hz', 'Đục lỗ (Nốt ruồi)', '50MP OIS (Chính) + 50MP OIS (Chân dung Tele) + 50MP (Góc rộng)', '4K', 'Camera 200MP tele zoom quang 3x, Cảm biến chính 50MP, Góc siêu rộng, Hỗ trợ AI chụp đêm, chân dung và chống rung OIS/EIS', '50MP', 'MediaTek Dimensity 9500', 'Mali-G1 Ultra MC12', 'Có', '5G', 'GPS, GLONASS, Galileo, Beidou', '12 GB', '256 GB', '7025mAh', 'Sạc nhanh có dây 80W Sạc không dây 50W', 'USB Type-C', 'ColorOS 16, nền tảng Android 16', '74.3 × 157.4 × 7.99 mm', '<210g', 'IP68/69', 'Loa kép', 'Cảm biến vân tay dưới màn hình', 'Cảm biến gia tốc, Cảm biến tiệm cận, Cảm biến ánh sáng, La bàn, Con quay hồi chuyển', 'Hỗ trợ 5G, Bảo mật vân tay, Nhận diện khuôn mặt, Kháng nước, kháng bụi, Điện thoại AI', 'Wi-Fi 6E (802.11 a/b/g/n/ac/ax)', '5.3', 'nan', 'nan', 'OPPO', 'Smartphone', NULL, '2025-10-29 01:56:47', 1),
('https://cellphones.com.vn/dien-thoai-honor-magic-v5.html', 'CellphoneS', 'HONOR Magic V5', '44.990.000đ', '6.43 inches', 'OLED', 'nan', 'Màn hình trong: 2352 x 2172 pixel Màn hình ngoài: 2376 x 1060 pixel 1,07 tỷ màu, Màn hình chăm sóc mắt cận thị AI, tần số 4320Hz PWM giảm nháy sáng, Điều chỉnh độ sáng động, Màn hình đêm theo nhịp sinh học, Màn hình tông màu tự nhiên', 'nan', 'nan', 'Camera góc siêu rộng 50MP (khẩu độ f/2.0) Camera góc rộng 50MP (khẩu độ f/1.6, OIS) Camera tele 64MP (khẩu độ f/2.5, OIS)', 'nan', 'nan', 'Màn hình trong: Camera góc rộng 20MP (f/2.2) Màn hình ngoài: Camera góc rộng 20MP (f/2.2)', 'Qualcomm Snapdragon 8 Elite', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '5820 mAh', 'nan', 'nan', 'MagicOS 9.0.1 (Dựa trên Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'SIM 1 + SIM 2 / SIM 1 + eSIM / 2 eSIM', 'Tám nhân (2×Prime 4,32 GHz+6×Performance 3,53 GHz)', 'HONOR', 'Foldable', 44990000, '2025-10-29 01:56:47', 2),
('https://cellphones.com.vn/dien-thoai-tecno-spark-40-pro-plus.html', 'CellphoneS', 'TECNO Spark 40 Pro+ 8GB 256GB (Chỉ có tại CellphoneS)', '5.190.000đ', '6.78 inches', 'AMOLED', 'nan', '1224 x 2720 pixels 6.78\'\' 144Hz & 1.5K 3D AMOLED Display', 'nan', 'nan', '50 MP', 'nan', 'nan', '13 MP', 'MediaTek Helio G200', 'nan', 'nan', 'nan', 'nan', '8 GB', '256 GB', '5200mAh, 45W', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'TECNO', 'Smartphone', 5190000, '2025-10-29 01:56:47', 3),
('https://cellphones.com.vn/dien-thoai-oppo-a6-pro.html', 'CellphoneS', 'OPPO A6 Pro 8GB 256GB', '7.990.000đ', '6.57 inches', 'AMOLED', '1080 x 2372 pixels (FullHD+)', '1 tỷ màu, Độ sáng tối đa 1400 nits', 'nan', 'nan', '50MP (Chính, f/1.8) + 2MP (Mono, f/2.4), có đèn flash', 'nan', 'nan', '16MP, f/2.4', 'MediaTek Helio G100, tối đa 2.2GHz', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '7000mAh', 'nan', 'nan', 'ColorOS 15 (Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân, tối đa 2.2GHz', 'OPPO', 'Smartphone', 7990000, '2025-10-29 01:56:47', 4),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-15c.html', 'CellphoneS', 'Xiaomi Redmi 15C 4GB 128GB NFC', '3.490.000đ', '6.9 inches', 'LCD', '1600 x 720 pixels (HD+)', 'Độ sáng 660 nits 810 nits (HBM) Tỉ lệ tương phản 1200:1 Cảm ứng 240Hz Đạt chứng nhận TÜV Rheinland: Giảm ánh sáng xanh, Không nhấp nháy, Thân thiện nhịp sinh học Hỗ trợ DC Dimming giảm nhấp nháy', 'nan', 'nan', '50MP, Ống kính -5P, f/1.8', 'nan', 'nan', '8MP f/2.0', 'MediaTek Helio G81-Ultra', 'nan', 'Có', 'nan', 'nan', '4 GB', '128 GB', '6000mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Octa-core (Cortex-A75 + Cortex-A55), tốc độ tối đa 2.0GHz', 'Xiaomi', 'Smartphone', 3490000, '2025-10-29 01:56:47', 5),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a17-5g.html', 'CellphoneS', 'Samsung Galaxy A17 5G 8GB 128GB', '5.890.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Độ sáng tối đa 800 nits Kính cường lực Corning Gorilla Glass 7', 'nan', 'nan', 'Chính 50 MP Phụ 5 MP, 2 MP', 'nan', 'nan', '13 MP', 'Exynos 1330', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân (2 nhân 2.4 GHz & 6 nhân 2.0 GHz)', 'Samsung', 'Smartphone', 5890000, '2025-10-29 01:56:47', 6),
('https://cellphones.com.vn/dien-thoai-nothing-phone-2a-plus.html', 'CellphoneS', 'Nothing Phone 2A Plus 5G 12GB 256GB - Chỉ có tại CellphoneS', '7.590.000đ', '6.7 inches', 'Flexible AMOLED On-cell', '1080 x 2412 pixels', '1300 nits 120 Hz, 1,07 tỷ màu', 'nan', 'nan', 'Camera chính: 50 MP, f/1.88, 1/1.57\" Camera góc siêu rộng: 50 MP, f/2.2, 1/2.76\"', 'nan', 'nan', '50 MP, f/2.2, 1/2.76\"', 'MediaTek Dimensity 7350 Pro 5G', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Up to 3.0 GHz', 'Nothing', 'Smartphone', 7590000, '2025-10-29 01:56:47', 7),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-13-pro-5g-8gb-256gb.html', 'CellphoneS', 'Xiaomi Redmi Note 13 Pro 5G 8GB 256GB', '6.450.000đ', '6.67 inches', 'AMOLED', '1220 x 2712 pixels', 'Tần số quét 120Hz, 1800 nits Kính cường lực Corning Gorilla Glass Victus', 'nan', 'nan', 'Chính 200 MP & Phụ 8 MP, 2 MP', 'nan', 'nan', '16 MP', 'Snapdragon 7s Gen 2 8 nhân', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5100 mAh', 'nan', 'nan', 'Android 13', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Dual nano-SIM hoặc 1 nano-SIM + 1 eSIM', '4 nhân 2.4 GHz & 4 nhân 1.95 GHz', 'Xiaomi', 'Smartphone', 6450000, '2025-10-29 01:56:47', 8),
('https://cellphones.com.vn/dien-thoai-tecno-camon-30s.html', 'CellphoneS', 'TECNO CAMON 30S 8GB 256GB', '3.990.000đ', '6.78 inches', 'AMOLED', '1080 x 2436 pixels', '120Hz Gorilla Glass 5', 'nan', 'nan', '50M OIS 2M + light sensor 100MP', 'nan', 'nan', '13MP', 'Mediatek G100', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5000mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'TECNO', 'Smartphone', 3990000, '2025-10-29 01:56:47', 9),
('https://cellphones.com.vn/dien-thoai-tecno-spark-30-pro-8gb-256gb.html', 'CellphoneS', 'TECNO SPARK 30 Pro 8GB 256GB Transformer - Chỉ có tại CellphoneS', '3.890.000đ', '6.78 inches', 'AMOLED', 'nan', '120Hz, 1700 nits', 'nan', 'nan', '108MP, f/1.75', 'nan', 'nan', '13MP', 'MediaTek Helio G100', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'TECNO', 'Smartphone', 3890000, '2025-10-29 01:56:47', 10),
('https://cellphones.com.vn/dien-thoai-honor-x9c-5g.html', 'CellphoneS', 'HONOR X9c 5G 12GB 256GB', '7.290.000đ', '6.78 inches', 'AMOLED', 'nan', '2700*1224 Pixels 1,07 tỷ màu 1.5 K độ phân giải 4000nit 100% DCI-P3', 'nan', 'nan', 'Camera chính 108MP (F1.75), OIS + Camera góc rộng 5MP (F2.2)', 'nan', 'nan', '16MP (F2.45)', 'Qualcomm Snapdragon 6 Gen 1', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6600mAh, 66W (120V/3.3A)', 'nan', 'nan', 'MagicOS 8.0, dựa trên Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', '4 x A78 *2.2GHz + 4 x A55*1.8GHz', 'HONOR', 'Smartphone', 7290000, '2025-10-29 01:56:47', 11),
('https://cellphones.com.vn/dien-thoai-xiaomi-15-12gb-512gb.html', 'CellphoneS', 'Xiaomi 15 5G 12GB 512GB', '20.990.000đ', '6.36 inches', 'CrystalRes AMOLED', '2670 x 1200 pixels', 'Độ sáng 3200 nits Tự động tinh chỉnh tần số quét 1-120Hz Tốc độ phản hồi cảm ứng lên đến 300Hz Hỗ trợ 68 tỷ màu Dải màu DCI-P3 Hỗ trợ Dolby Vision, HDR10+, HDR10 Độ sáng DC Công nghệ cảm ứng ướt', 'nan', 'nan', 'Camera chính Leica: 50MP, ƒ/1.62, OIS, Tiêu cự tương đương 23mm Camera tele Leica: 50MP, ƒ/2.0, OIS Camera góc siêu rộng Leica: 50MP, ƒ/2.2, 115°', 'nan', 'nan', 'Camera trước: 32MP, ƒ/2.0, 90°, Tiêu cự 21mm', 'Snapdragon 8 Elite (Tiến trình sản xuất 3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '512 GB', '5240 mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Sim kép (nano-Sim và e-Sim) - Hỗ trợ 2 e-Sim', '2 x Lõi Prime, tốc độ tối đa 4,32GHz 6 x Lõi hiệu suất, lên đến 3,53GHz', 'Xiaomi', 'Smartphone', 20990000, '2025-10-29 01:56:47', 12),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a06-5g.html', 'CellphoneS', 'Samsung Galaxy A06 5G 4GB 128GB', '3.120.000đ', '6.7 inches', 'PLS LCD', '720 x 1600 pixel', '16 triệu màu, 60Hz, 576 nits', 'nan', 'nan', '50.0 MP, f/1.8 2.0 MP, f/2.4', 'nan', 'nan', '8.0 MP, f/2.0', 'MediaTek Dimensity 6300', 'nan', 'nan', 'nan', 'nan', '4 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '2 nhân 2.0 GHz & 6 nhân 1.8 GHz', 'Samsung', 'Smartphone', 3120000, '2025-10-29 01:56:47', 13),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-14-pro-plus.html', 'CellphoneS', 'Xiaomi Redmi Note 14 Pro Plus 5G 8GB 256GB', '8.590.000đ', '6.67 inches', 'AMOLED', '2712 x 1220 pixels', 'Tần số quét: Lên đến 120Hz Độ sáng: 3000 nits Độ sâu màu: 12-bit Tỷ lệ tương phản: 5,000,000:1', 'nan', 'nan', 'Chính 200MP, OIS - f/1.65 Góc siêu rộng 8MP - f/2.2 Macro 2MP - f/2.4', 'nan', 'nan', 'Camera trước - f/2.2', 'Snapdragon® 7s Gen 3', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5110 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, xung nhịp 2.5Ghz', 'Xiaomi', 'Smartphone', 8590000, '2025-10-29 01:56:47', 14),
('https://cellphones.com.vn/dien-thoai-xiaomi-poco-x7-5g.html', 'CellphoneS', 'Xiaomi POCO X7 5G 12GB 512GB', '7.750.000đ', '6.67 inches', 'AMOLED', '2712 x 1220 pixels', 'Tần số quét: 120Hz, 500 nits, Độ sâu màu: 12bit, Tỷ lệ tương phản: 5.000.000:1, DCI-P3, Kính Corning® Gorilla® Victus® 2, Dolby Vision, HDR10+', 'nan', 'nan', 'Camera chính: 50MP, f/1.5 Camera góc siêu rộng: 8MP, f/2.2 Camera macro: 2MP, f/2.4', 'nan', 'nan', '20MP, f/2.2', 'Powerful Dimensity 7300-Ultra', 'nan', 'Có', 'nan', 'nan', '12 GB', '512 GB', '5110mAh', 'nan', 'nan', 'Xiaomi HyperOS', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'Octa-core CPU, up to 2.5GHz', 'Xiaomi', 'Smartphone', 7750000, '2025-10-29 01:56:47', 15),
('https://cellphones.com.vn/dien-thoai-tecno-pova-7-256gb.html', 'CellphoneS', 'Tecno Pova 7 8GB 256GB', '4.490.000đ', '6.78 inches', 'IPS LCD', '1080x2460 pixels', 'Tần số quét 120Hz, FHD+, Độ sáng tối đa 900nits', 'nan', 'nan', '108M+2M', 'nan', 'nan', '8MP', 'MediaTek Helio G100 Utimate', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '7000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2*A76 up to 2.2Ghz, 6*A55 up to 2.0Ghz', 'TECNO', 'Smartphone', 4490000, '2025-10-29 01:56:47', 16),
('https://cellphones.com.vn/dien-thoai-tecno-pova-7.html', 'CellphoneS', 'Tecno Pova 7 8GB 128GB', '3.990.000đ', '6.78 inches', 'IPS LCD', '1080x2460 pixels', 'Tần số quét 120Hz, FHD+, Độ sáng tối đa 900nits', 'nan', 'nan', '108M+2M', 'nan', 'nan', '8MP', 'MediaTek Helio G100 Utimate', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '7000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2*A76 up to 2.2Ghz, 6*A55 up to 2.0Ghz', 'TECNO', 'Smartphone', 3990000, '2025-10-29 01:56:47', 17),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a26.html', 'CellphoneS', 'Samsung Galaxy A26 5G 8GB 128GB', '6.270.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Tần số quét 120Hz, 800 nits, Mặt kính Gorilla Glass Victus', 'nan', 'nan', '50MP + 8MP + 2MP', 'nan', 'nan', '13MP', 'Exynos 1380 (Quartz)', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '2.4GHz,2GHz, Octa-Core', 'Samsung', 'Smartphone', 6270000, '2025-10-29 01:56:47', 18),
('https://cellphones.com.vn/dien-thoai-poco-x7-pro-5g.html', 'CellphoneS', 'Xiaomi POCO X7 Pro 5G 12GB 256GB - Chỉ có tại CellphoneS', '9.090.000đ', '6.67 inches', 'AMOLED', '1220 x 2712 pixels', '3200 nits, Tần số quét 120Hz, Dolby Vison', 'nan', 'nan', '50MP', 'nan', 'nan', 'nan', 'Dimensity 8400-Ultra', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000 mAh', 'nan', 'nan', 'Android 14', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'nan', 'Xiaomi', 'Smartphone', 9090000, '2025-10-29 01:56:47', 19),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a07.html', 'CellphoneS', 'Samsung Galaxy A07 4GB 128GB', '3.190.000đ', '6.7 inches', 'IPS LCD', '720 x 1600 pixel', 'Độ sáng tối đa 480 nits Kính cường lực Panda', 'nan', 'nan', 'Chính 50 MP Phụ 2 MP', 'nan', 'nan', '8 MP', 'MediaTek Helio G99', 'nan', 'Không', 'nan', 'nan', '4 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân (2 nhân 2.2 GHz & 6 nhân 2.0 GHz)', 'Samsung', 'Smartphone', 3190000, '2025-10-29 01:56:47', 20),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a56.html', 'CellphoneS', 'Samsung Galaxy A56 5G 8GB 128GB', '9.310.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', '16 triệu màu, 120 Hz, 1200 nit, kính cường lực Corning Gorilla Glass Victus+', 'nan', 'nan', '50.0 MP, F/1.8 + 12.0 MP, F/2.2 + 5.0 MP, F/2.4', 'nan', 'nan', '12.0 MP, F/2.2', 'Exynos 1580', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000 mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'SIM 1 + SIM 2 / SIM 1 + eSIM / 2 eSIM', '8 nhân 2.9GHz, 2.6GHz, 1.9GHz', 'Samsung', 'Smartphone', 9310000, '2025-10-29 01:56:47', 21),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-a17.html', 'CellphoneS', 'Samsung Galaxy A17 8GB 128GB', '4.990.000đ', '6.7 inches', 'Super AMOLED', '1080 x 2340 pixels (FullHD+)', 'Độ sáng tối đa 800 nits Kính cường lực Corning Gorilla Glass 7', 'nan', 'nan', 'Chính 50 MP Phụ 5 MP, 2 MP', 'nan', 'nan', '13 MP', 'MediaTek Helio G99', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', 'Li-Ion 5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM (Sim 2 chung khe với thẻ nhớ)', '8 nhân (2 nhân 2.2GHz & 6 nhân 2.0GHz)', 'Samsung', 'Smartphone', 4990000, '2025-10-29 01:56:47', 22),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-s25-ultra.html', 'CellphoneS', 'Samsung Galaxy S25 Ultra 12GB 256GB', '27.280.000đ', '6.9 inches', 'Dynamic AMOLED 2X', '3120 x 1440 pixels (Quad HD+)', '120Hz 2600 nits Corning® Gorilla® Armor 2', 'nan', 'nan', 'Camera siêu rộng 50MP Camera góc rộng 200 MP Camera Tele (5x) 50MP Camera Tele (3x) 10MP\"', 'nan', 'nan', '12 MP', 'Snapdragon 8 Elite dành cho Galaxy (3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM + eSIM', 'nan', 'Samsung', 'Smartphone', 27280000, '2025-10-29 01:56:47', 23),
('https://cellphones.com.vn/dien-thoai-xiaomi-15t-pro-5g.html', 'CellphoneS', 'Xiaomi 15T Pro 5G 12GB 512GB', '19.490.000đ', '6.83 inches', 'AMOLED', 'nan', 'Độ sáng tối đa 3200 nits, 447ppi, 68 tỷ màu, DCI-P3, HDR10+, Dolby Vision®, DC dimming, TÜV Rheinland', 'nan', 'nan', 'Chính: 50MP, f/1.62, OIS, cảm biến Light Fusion 900 Tele: 50MP, f/3.0, OIS, Samsung JN5, Zoom 5x, 10x lossless, Ultra Zoom 20x Siêu rộng: 12MP, f/2.2, 120° FOV', 'nan', 'nan', '32MP, f/2.2, tiêu cự 21mm', 'MediaTek Dimensity 9400+', 'nan', 'nan', 'nan', 'nan', '12 GB', '512 GB', '5500mAh', 'nan', 'nan', 'Xiaomi HyperOS 2', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Xiaomi', 'Smartphone', 19490000, '2025-10-29 01:56:47', 24),
('https://cellphones.com.vn/dien-thoai-xiaomi-15t-5g.html', 'CellphoneS', 'Xiaomi 15T 5G 12GB 512GB', '14.990.000đ', '6.83 inches', 'AMOLED', 'nan', 'Độ sáng tối đa 3200 nits (25% vùng hiển thị) 68 tỷ màu DCI-P3 Chứng nhận TÜV Rheinland (Low Blue Light, Flicker Free, Circadian Friendly)', 'nan', 'nan', 'Chính: 50MP (23mm, f/1.7, OIS, Light Fusion 800) Tele: 50MP (46mm, f/1.9) Góc siêu rộng: 12MP (15mm, f/2.2, 120° FOV)', 'nan', 'nan', '32MP, f/2.2, tiêu cự 21mm', 'MediaTek Dimensity 8400-Ultra', 'nan', 'nan', 'nan', 'nan', '12 GB', '512 GB', '5500mAh', 'nan', 'nan', 'Xiaomi HyperOS 2, tích hợp Xiaomi HyperAI & Google Gemini', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Nano-SIM', 'nan', 'Xiaomi', 'Smartphone', 14990000, '2025-10-29 01:56:47', 25),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-z-flip-7.html', 'CellphoneS', 'Samsung Galaxy Z Flip7 12GB 256GB', '25.990.000đ', '6.9 inches', 'Dynamic AMOLED 2X', 'nan', 'Tần số quét: 120 Hz Độ phân giải màn hình chính: 2520 x 1080 (FHD+) Kích cỡ màn hình phụ: 4.1\" Độ phân giải màn hình phụ: 1048 x 948 Công nghệ màn hình phụ: Super AMOLED Độ sâu màu sắc: 16M', 'nan', 'nan', '50 MP, F1.8 + 12 MP, F2.2', 'nan', 'nan', '10MP, F2.2', 'nan', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4300mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '10 nhân, 3.3 GHz, 2.74GHz, 2.36GHz, 1.8GHz', 'Samsung', 'Foldable', 25990000, '2025-10-29 01:56:47', 26),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-z-fold-7.html', 'CellphoneS', 'Samsung Galaxy Z Fold7 12GB 256GB', '42.990.000đ', '8.0 inches', 'Dynamic AMOLED 2X', 'nan', 'Tần số quét: 120 Hz Độ phân giải màn hình chính: 2184 x 1968 (QXGA+) Kích cỡ màn hình phụ: 6.5\" Độ phân giải màn hình phụ: 2520 x 1080 (FHD+) Độ sâu màu sắc: 16M', 'nan', 'nan', '200 MP, F1.7 + 12 MP, F2.2 + 10 MP, F2.4', 'nan', 'nan', '10MP, F2.2', 'Snapdragon® 8 Elite 3nm for Galaxy', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4400mAh', 'nan', 'nan', 'Android', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, 4.47GHz, 3.5GHz', 'Samsung', 'Foldable', 42990000, '2025-10-29 01:56:47', 27),
('https://cellphones.com.vn/dien-thoai-samsung-galaxy-s25.html', 'CellphoneS', 'Samsung Galaxy S25 256GB', '16.690.000đ', '6.2 inches', 'Dynamic AMOLED 2X', '2340 x 1080-pixel', '120Hz 2600 nits Corning® Gorilla® Armor 2', 'nan', 'nan', 'Camera siêu rộng 12MP Camera góc rộng 50MP Camera Tele 10MP', 'nan', 'nan', '12MP', 'Snapdragon 8 Elite dành cho Galaxy (3nm)', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '4000 mAh', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano SIM hoặc 2 eSIM hoặc 1 Nano SIM + 1 eSIM', 'Tốc độ CPU 4.47GHz, 3.5GHz 8 nhân', 'Samsung', 'Smartphone', 16690000, '2025-10-29 01:56:47', 28),
('https://cellphones.com.vn/dien-thoai-oppo-find-n5.html', 'CellphoneS', 'OPPO FIND N5', '44.180.000đ', '8.12 inches', 'AMOLED', '2.480 x 2.248 pixels', 'Tần số: 120Hz Màn hình chính: Kính Pet+UTG, 1400nit Màn hình ngoài: 6.62 inch, 1140 x 2616 (FHD+), kính siêu mỏng Nanocrytal, 1600nit 1 tỷ màu sắc', 'nan', 'nan', '50MP, ƒ/1.8 OIS (Chính) + 8MP, ƒ/2.2 (Góc rộng) + 50MP, ƒ/2.7 (Tele)', 'nan', 'nan', 'Màn hình chính: 8MP, ƒ/2.4 Màn hình ngoài: 8MP, ƒ/2.4', 'Qualcomm Snapdragon® 8 Elite', 'nan', 'Có', 'nan', 'nan', '16 GB', '512 GB', '5600mAh', 'nan', 'nan', 'ColorOS 15, nền tảng Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'Dual nano-SIM hoặc 1 nano-SIM + 1 eSIM', 'nan', 'OPPO', 'Smartphone', 44180000, '2025-10-29 01:56:47', 29),
('https://cellphones.com.vn/dien-thoai-nubia-neo-3-gt-12gb-256gb.html', 'CellphoneS', 'Nubia Neo 3 GT 12GB 256GB', '6.590.000đ', '6.8 inches', 'OLED', '1080 x 2392 pixels', 'FullHD+ Tần số 120Hz Độ sáng cực đại 1300 nit Gam màu DCI-P3 100% Tỷ lệ tương phản 1000000:1 Tần số lấy mẫu cảm ứng tức thời 1200Hz', 'nan', 'nan', '50MP + 2MP', 'nan', 'nan', '16MP', 'Unisoc T9100', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'Nubia', 'Smartphone', 6590000, '2025-10-29 01:56:47', 30),
('https://cellphones.com.vn/dien-thoai-oppo-reno14.html', 'CellphoneS', 'OPPO Reno14 5G 12GB 256GB', '15.700.000đ', '6.59 inches', 'AMOLED', '1256 x 2760 pixels', 'Tần số quét 120Hz, độ sáng 1200 nits, kính GG7i', 'nan', 'nan', 'Chính 50MP OIS, F/1.8 Tele 50MP OIS, F/2.8 Góc rộng 8MP, F/2.2', 'nan', 'nan', '50MP, F/2.0', 'MediaTek Dimensity 8350 5G, tối đa 3.35GHz', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '6000mAh', 'nan', 'nan', 'ColorOS 15, Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', '8 nhân, 3.35GHz', 'OPPO', 'Smartphone', 15700000, '2025-10-29 01:56:47', 31),
('https://cellphones.com.vn/dien-thoai-xiaomi-redmi-note-14-5g.html', 'CellphoneS', 'Xiaomi Redmi Note 14 5G 8GB 256GB', '6.790.000đ', '6.67 inches', 'AMOLED', '1080 x 2400 pixels (FullHD+)', 'Tần số quét 120Hz Độ sáng 2100 nits Độ sâu màu 8 bit Độ tương phản 5.000.000:1 Giải màu rộng DCI-P3 960Hz PWM dimming Corning Gorilla Glass 5', 'nan', 'nan', '108MP, ƒ/1.7 8MP góc siêu rộng, ƒ/2.2 2MP Macro, ƒ/2.4', 'nan', 'nan', '20MP, ƒ/2.2', 'MediaTek Dimensity 7025 - Ultra', 'nan', 'Có', 'nan', 'nan', '8 GB', '256 GB', '5110mAh', 'nan', 'nan', 'Xiaomi HyperOS', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Octa-core processor, up to 2.5GHz Tiền trình 6nm', 'Xiaomi', 'Smartphone', 6790000, '2025-10-29 01:56:47', 32),
('https://cellphones.com.vn/dien-thoai-sony-xperia-1-vii.html', 'CellphoneS', 'Sony Xperia 1 VII 12GB 256GB', '33.490.000đ', '6.5 inches', 'OLED', '1080 x 2340 pixels (FullHD+)', 'DCI-P3 100%, BT.2020, 10-bit, cảm biến ánh sáng trước/sau', 'nan', 'nan', 'Chính 52MP, f/1.9 Tele 12MP, f/2.3–f/3.5 Góc siêu rộng 50MP, f/2.0', 'nan', 'nan', '12MP, f/2.0', 'Snapdragon® 8 Elite', 'nan', 'Có', 'nan', 'nan', '12 GB', '256 GB', '5.000 mAh', 'nan', 'nan', 'Android 15', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 SIM (Nano-SIM)', 'Kryo', 'Sony', 'Smartphone', 33490000, '2025-10-29 01:56:47', 33),
('https://cellphones.com.vn/dien-thoai-nothing-phone-3a.html', 'CellphoneS', 'Nothing Phone 3A 8GB 128GB', '8.490.000đ', '6.77 inches', 'AMOLED', '1080 x 2392 pixels', '120Hz, NTSC 105%, 387 ppi, 3000nit, 1.07 tỷ màu, DCI-P3 100%, HDR10+', 'nan', 'nan', 'Camera chính: 50MP, F1.88 Camera góc siêu rộng: 8MP, F2.2 Camera Tele: 50MP, F2.0', 'nan', 'nan', '32MP, F2.2', 'Qualcomm SM7635 Snapdragon 7s Gen 3', 'nan', 'Có', 'nan', 'nan', '8 GB', '128 GB', '5000mAh', 'nan', 'nan', 'Nothing OS 3.1 (Based on Android 15)', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', '2 Nano-SIM', 'nan', 'Nothing', 'Smartphone', 8490000, '2025-10-29 01:56:47', 34);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `etl_log`
--
ALTER TABLE `etl_log`
  ADD PRIMARY KEY (`etl_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `etl_log`
--
ALTER TABLE `etl_log`
  MODIFY `etl_id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
