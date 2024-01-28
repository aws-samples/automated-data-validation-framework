CREATE EXTERNAL TABLE `griffin_datavalidation_blog.accuracy_detailed_validation`(
  `name` string COMMENT 'from deserializer',
  `tmst` bigint COMMENT 'from deserializer',
  `value` struct<source_count:int,data_mismatched_records:int,data_matched_records:int,matchedfraction:float> COMMENT 'from deserializer',
  `applicationid` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'paths'='applicationId,name,tmst,value')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<bucket_name>/accuracy/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1639642091');

DROP TABLE IF EXISTS griffin_datavalidation_blog.accuracy_summary_validation;

CREATE EXTERNAL TABLE `griffin_datavalidation_blog.accuracy_summary_validation`(
    `name` string COMMENT 'from deserializer',
    `tmst` bigint COMMENT 'from deserializer',
    `value` struct<source_count:int,target_count:int> COMMENT 'from deserializer',
    `applicationid` string COMMENT 'from deserializer')
  ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
  WITH SERDEPROPERTIES (
    'paths'='applicationId,name,tmst,value')
  STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  LOCATION
    's3://<bucket_name>/total_count/'
  TBLPROPERTIES (
    'transient_lastDdlTime'='1637005278');

CREATE OR REPLACE VIEW griffin_datavalidation_blog.accuracy_detailed_view AS
    SELECT
      name Table_Name
    , value.source_count source_count
    , value.data_mismatched_records data_mismatched_records
    , value.data_matched_records data_matched_records
    , round((CAST(value.matchedfraction AS double) * 100), 2) Matched_Percent
    , applicationid applicationid
    , from_unixtime(CAST((tmst / 1000) AS bigint)) Create_ts
    FROM
      griffin_datavalidation_blog.accuracy_detailed_validation;


CREATE OR REPLACE VIEW griffin_datavalidation_blog.accuracy_summary_view AS
  SELECT
    name Table_Name
  , value.source_count source_count
  , value.target_count Target_Count
  , (value.source_count - value.target_count) Row_Count_Difference
  , round(((CAST((value.source_count - value.target_count) AS double) / CAST(value.source_count AS double)) * 100), 2) Row_Count_Difference_Percent
  , applicationid applicationid
  , from_unixtime(CAST((tmst / 1000) AS bigint)) Create_ts
  FROM
   griffin_datavalidation_blog.accuracy_summary_validation;

