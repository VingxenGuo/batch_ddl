CREATE DATABASE  IF NOT EXISTS `tool` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `tool`;
-- MySQL dump 10.13  Distrib 8.0.31, for macos12 (x86_64)
--
-- Host: 192.168.185.119    Database: tool
-- ------------------------------------------------------
-- Server version	8.0.21

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `batch_copy`
--

DROP TABLE IF EXISTS `batch_copy`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `batch_copy` (
  `connection_id` int DEFAULT NULL,
  `exec_table` varchar(64) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `start_ts` timestamp NULL DEFAULT NULL,
  `finish_time` timestamp NULL DEFAULT NULL,
  `duration_ts` double DEFAULT NULL,
  `affect_rows` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `batch_copy`
--

LOCK TABLES `batch_copy` WRITE;
/*!40000 ALTER TABLE `batch_copy` DISABLE KEYS */;
/*!40000 ALTER TABLE `batch_copy` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'tool'
--
/*!50003 DROP PROCEDURE IF EXISTS `copyBatches` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `copyBatches`(IN your_target_table VARCHAR(64),IN your_source_table VARCHAR(64), IN your_condition VARCHAR(64), IN batch_size INT)
BEGIN
    DECLARE DOne INT DEFAULT FALSE;
    DECLARE batch_size INT; -- 設定每批次的大小
    DECLARE startIdx INT DEFAULT 0;
    DECLARE endIdx INT DEFAULT 0;
    DECLARE your_condition VARCHAR(64);
    DECLARE your_variables VARCHAR(64);
    DECLARE e_sql TEXT DEFAULT '';
    DECLARE sql_1 TEXT;
    DECLARE max_id INT;
    DECLARE duration_ts DOUBLE;
    DECLARE current_ts DOUBLE;
    DECLARE start_ts DOUBLE;
    DECLARE last_exec_ts DOUBLE;
    DECLARE affect_rows INT;
	
    IF ISNULL(your_condition) THEN
		SET your_condition = 1;
    END IF;
    IF ISNULL(batch_size) THEN
		SET batch_size = 10000;
    END IF;
    
    # 創建 target table 結構等同 source table
    SET @exec_sql = CONCAT('CREATE table IF NOT EXISTS ', your_target_table, ' like ', your_source_table);
    PREPARE stmt FROM @exec_sql;
    EXECUTE  stmt;
    DEALLOCATE PREPARE stmt;
    
    # 取得 source table 的 max id 作為後續條件判斷停止的參數
    SET @sql_1 = CONCAT('SELECT id INTO @max_id FROM ', your_source_table, ' order by id desc limit 1;');
    PREPARE stmt FROM @sql_1;
    EXECUTE  stmt;
    DEALLOCATE PREPARE stmt;
    SET max_id = @max_id;
	
	-- 若是  INSERT時插入到目標表，這裡假設目標表為 your_target_table
	WHILE max_id > endIdx DO
    
		SET endIdx = endIdx + batch_size ;
		SET @e_sql = CONCAT('INSERT INTO ', your_target_table, ' SELECT * FROM ', your_source_table, ' WHERE ', your_condition, ' AND id BETWEEN ', startIdx, ' AND ', endIdx, ';');
        SET start_ts = UNIX_TIMESTAMP(NOW(6));
        PREPARE stmt FROM @e_sql;
        EXECUTE  stmt;
        SET current_ts = UNIX_TIMESTAMP(NOW(6));
        SET duration_ts = current_ts - start_ts;
        SET affect_rows = ROW_COUNT();
        DEALLOCATE PREPARE stmt;
        INSERT tool.batch_copy(connection_id, exec_table, start_ts, finish_time, duration_ts, affect_rows) VALUES (CONNECTION_ID(), your_target_table, FROM_UNIXTIME(start_ts), FROM_UNIXTIME(current_ts), duration_ts, affect_rows);
		SET startIdx = endIdx + 1;
		
	END WHILE;
    

    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `copyInsertBatches` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `copyInsertBatches`(IN batch_size INT, IN your_target_table varchar(64),IN your_source_table varchar(64), IN your_condition varchar(64) )
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE batch_size INT DEFAULT 10000; -- 設定每批次的大小
    DECLARE startIdx INT DEFAULT 0;
    DECLARE endIdx INT DEFAULT 0;
    DECLARE your_condition varchar(64) DEFAULT '1';
    DECLARE your_variables varchar(64);
    DECLARE e_sql text default '';
    DECLARE sql_1 text;
    DECLARE max_id int;
    DECLARE duration_ts timestamp;
    DECLARE current_ts timestamp;
    DECLARE start_ts timestamp;
    DECLARE last_exec_ts timestamp;
    DECLARE affect_rows int;
    
    
    
    set @sql_1 = concat('select id into @max_id from ', your_source_table, ' order by id desc limit 1;');
    prepare stmt from @sql_1;
    execute stmt;
    deallocate prepare stmt;
    set max_id = @max_id;
    select max_id;
	
	-- 若是 insert 時插入到目標表，這裡假設目標表為 your_target_table
	while max_id > endIdx do
    
		set endIdx = endIdx + batch_size ;
		set @e_sql = concat('INSERT INTO ', your_target_table, ' SELECT * FROM ', your_source_table, ' WHERE ', your_condition, ' AND id BETWEEN ', startIdx, ' AND ', endIdx, ';');
		select @e_sql;
        set start_ts = UNIX_TIMESTAMP(NOW(6));
        prepare stmt from @e_sql;
        execute stmt;
        set current_ts = UNIX_TIMESTAMP(NOW(6));
        set duration_ts = current_ts - start_ts;
        deallocate prepare stmt;
        insert tool.batch_copy(connection_id, exec_table, start_tx, last_exec_ts, duration_ts, affect_rows) values (CONNECTION_ID(), exec_table, start_tx, last_exec_ts, duration_ts, ROW_COUNT());
		set startIdx = endIdx + 1;
		
	end while;
    

    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `InsertInBatches` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `InsertInBatches`(IN batch_size INT, IN your_target_table varchar(64),IN your_source_table varchar(64), IN your_condition varchar(64) )
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE batch_size INT DEFAULT 10000; -- 設定每批次的大小
    DECLARE startIdx INT DEFAULT 0;
    DECLARE endIdx INT DEFAULT 0;
    DECLARE your_condition varchar(64) DEFAULT '1';
    DECLARE your_variables varchar(64);
    DECLARE e_sql text default '';
    DECLARE sql_1 text;
    DECLARE max_id int;
    
    
    
#    DECLARE cur CURSOR FOR SELECT * FROM detail_record.bjl_gameRecord WHERE your_condition;
#    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    set @sql_1 = concat('select id into @max_id from ', your_source_table, ' order by id desc limit 1;');
    prepare stmt from @sql_1;
    execute stmt;
    deallocate prepare stmt;
    select @sql_1;
    set max_id = @max_id;
    select max_id;
    

	
	-- 若是 insert 時插入到目標表，這裡假設目標表為 your_target_table
	while max_id > endIdx do
		set endIdx = endIdx + batch_size ;
		set e_sql = concat(e_sql ,'\nINSERT INTO ', your_target_table, ' SELECT * FROM ', your_source_table, ' WHERE ', your_condition, ' AND id BETWEEN ', startIdx, ' AND ', endIdx, ';');
	
		set startIdx = endIdx + 1;
		
	end while;
    select e_sql;
        -- 這裡加入 UPDATE 或 DELETE 的邏輯

    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-02-23 16:28:42
