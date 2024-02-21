create database if not exists tool ;
use tool;
drop procedure if exists copyInsertBatches;
DELIMITER //

CREATE PROCEDURE copyInsertBatches(IN batchSize INT, IN your_target_table varchar(64),IN your_source_table varchar(64), IN your_condition varchar(64) )
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE batchSize INT DEFAULT 10000; -- 設定每批次的大小
    DECLARE startIdx INT DEFAULT 0;
    DECLARE endIdx INT DEFAULT 0;
    DECLARE your_condition varchar(64) DEFAULT '1';
    DECLARE your_variables varchar(64);
    DECLARE e_sql text default '';
    DECLARE sql_1 text;
    DECLARE max_id int;
    DECLARE duration_ts timestamp;
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
    
		set endIdx = endIdx + batchSize ;
		set @e_sql = concat('INSERT INTO ', your_target_table, ' SELECT * FROM ', your_source_table, ' WHERE ', your_condition, ' AND id BETWEEN ', startIdx, ' AND ', endIdx, ';');
		select @e_sql;
        set start_ts = UNIX_TIMESTAMP(NOW(6));
        prepare stmt from @e_sql;
        execute stmt;
        set current_ts = UNIX_TIMESTAMP(NOW(6));
        set duration_ts = current_ts - start_ts;
        set affect_rows = ROW_COUNT();
        deallocate prepare stmt;
        insert tool.batch_copy(start_tx, last_exec_ts, duration_ts, exec_table, affect_rows) values (start_tx, last_exec_ts, duration_ts, exec_table, affect_rows );
		set startIdx = endIdx + 1;
		
	end while;
    

    
END //

DELIMITER ;

