CREATE TABLE [sandbox_ebi].[datalineage_informationjobs] (
    [udw_job]                VARCHAR (150) NULL,
    [udw_workload_type]      VARCHAR (150) NULL,
    [udw_job_type]           VARCHAR (150) NULL,
    [source_system]          VARCHAR (150) NULL,
    [source_table_name]      VARCHAR (250) NULL,
    [udw_stage_table_schema] [sysname]     NULL,
    [udw_stage_table_name]   [sysname]     NULL,
    [udw_table_schema]       [sysname]     NULL,
    [udw_table_name]         [sysname]     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

