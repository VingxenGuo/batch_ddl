CREATE DATABASE IF NOT EXISTS tool ;
USE tool;
DROP PROCEDURE IF EXISTS  copyBatches;
DELIMITER //

CREATE PROCEDURE copyBatches(IN your_target_table VARCHAR(64),IN your_source_table VARCHAR(64), IN your_condition VARCHAR(64), IN batch_size INT)
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
    

    
END //

DELIMITER ;

