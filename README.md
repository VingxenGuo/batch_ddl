# batch_ddl
## copyBatches procedure
1. call 時輸入 input 參數如 call(IN your_target_table VARCHAR(64),IN your_source_table VARCHAR(64), IN your_condition VARCHAR(64), IN batch_size INT)
2. your_target_table 及 your_source_table 必須填入單引號刮起來的string，string 需含 schema, table，如 'example_schema.example_table'。your_condition 及 batct_size 如沒有可輸入 null。
3. copy 中可以去 tool.batch_copy 查看 copy 情形，若是有卡住等情況可查看該 connection_id 欄位，下 kill $connection_id ，緊急停止 copy。