use testdb;
drop procedure if exists InsertInBatches;
DELIMITER //

CREATE PROCEDURE InsertInBatches(IN batchSize INT, IN your_target_table varchar(64),IN your_source_table varchar(64), IN your_condition varchar(64) )
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
		set endIdx = endIdx + batchSize ;
		set e_sql = concat(e_sql ,'\nINSERT INTO ', your_target_table, ' SELECT * FROM ', your_source_table, ' WHERE ', your_condition, ' AND id BETWEEN ', startIdx, ' AND ', endIdx, ';');
	
		set startIdx = endIdx + 1;
		
	end while;
    select e_sql;
        -- 這裡加入 UPDATE 或 DELETE 的邏輯

    
END //

DELIMITER ;

