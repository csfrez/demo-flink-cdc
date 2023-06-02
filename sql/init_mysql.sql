-- --------------------------------------------------------
-- 主机:                           10.11.0.142
-- 服务器版本:                        5.7.35-log - MySQL Community Server (GPL)
-- 服务器操作系统:                      Linux
-- HeidiSQL 版本:                  12.0.0.6468
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- 导出 mydb 的数据库结构
CREATE DATABASE IF NOT EXISTS `mydb` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `mydb`;

-- 导出  表 mydb.enriched_orders 结构
DROP TABLE IF EXISTS `enriched_orders`;
CREATE TABLE IF NOT EXISTS `enriched_orders` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` datetime NOT NULL,
  `customer_name` varchar(255) NOT NULL,
  `price` decimal(10,5) NOT NULL,
  `product_id` int(11) NOT NULL,
  `order_status` tinyint(1) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `description` varchar(512) DEFAULT NULL,
  `shipment_id` int(11) DEFAULT NULL,
  `origin` varchar(255) DEFAULT NULL,
  `destination` varchar(255) DEFAULT NULL,
  `is_arrived` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10001 DEFAULT CHARSET=latin1;

-- 导出  表 mydb.orders 结构
DROP TABLE IF EXISTS `orders`;
CREATE TABLE IF NOT EXISTS `orders` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` datetime NOT NULL,
  `customer_name` varchar(255) NOT NULL,
  `price` decimal(10,5) NOT NULL,
  `product_id` int(11) NOT NULL,
  `order_status` tinyint(1) NOT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10008 DEFAULT CHARSET=latin1;

-- 正在导出表  mydb.orders 的数据：~7 rows (大约)
DELETE FROM `orders`;
INSERT INTO `orders` (`order_id`, `order_date`, `customer_name`, `price`, `product_id`, `order_status`) VALUES
	(10001, '2020-07-30 10:08:22', 'Jark', 50.50000, 102, 0),
	(10002, '2020-07-30 10:11:09', 'Sally', 15.00000, 105, 0),
	(10003, '2020-07-30 12:00:30', 'Edward', 25.25000, 106, 0),
	(10004, '2020-07-30 12:00:30', 'Edward', 25.25000, 106, 0),
	(10005, '2023-06-01 11:45:50', 'Frez1', 29.71000, 105, 0),
	(10006, '2020-07-30 12:00:30', 'Frez', 25.25000, 106, 0),
	(10007, '2020-07-30 12:00:30', 'Edward1', 120.00000, 106, 0);

-- 导出  表 mydb.products 结构
DROP TABLE IF EXISTS `products`;
CREATE TABLE IF NOT EXISTS `products` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `description` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=latin1;

-- 正在导出表  mydb.products 的数据：~9 rows (大约)
DELETE FROM `products`;
INSERT INTO `products` (`id`, `name`, `description`) VALUES
	(101, 'scooter', 'Small 2-wheel scooter'),
	(102, 'car battery', '12V car battery'),
	(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3'),
	(104, 'hammer', '12oz carpenter\'s hammer'),
	(105, 'hammer', '14oz carpenter\'s hammer'),
	(106, 'hammer', '16oz carpenter\'s hammer'),
	(107, 'rocks', 'box of assorted rocks'),
	(108, 'jacket', 'water resistent black wind breaker'),
	(109, 'spare tire', '24 inch spare tire');

-- 导出  表 mydb.shipments 结构
DROP TABLE IF EXISTS `shipments`;
CREATE TABLE IF NOT EXISTS `shipments` (
  `shipment_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_id` int(11) NOT NULL,
  `origin` varchar(255) NOT NULL,
  `destination` varchar(255) NOT NULL,
  `is_arrived` tinyint(1) NOT NULL,
  PRIMARY KEY (`shipment_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10005 DEFAULT CHARSET=latin1;

-- 正在导出表  mydb.shipments 的数据：~3 rows (大约)
DELETE FROM `shipments`;
INSERT INTO `shipments` (`shipment_id`, `order_id`, `origin`, `destination`, `is_arrived`) VALUES
	(10001, 10001, 'Beijing', 'Shanghai', 0),
	(10002, 10002, 'Hangzhou', 'Shanghai', 0),
	(10003, 10003, 'Shanghai', 'Hangzhou', 0),
	(10004, 10007, 'Shenzhen', 'Hangzhou', 0);

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
