CREATE TABLE [sandbox_ebi].[datalineage_systematicmapping] (
    [DataSet]                VARCHAR (42)   NOT NULL,
    [udw_job]                VARCHAR (150)  NULL,
    [udw_workload_type]      VARCHAR (150)  NULL,
    [udw_job_type]           VARCHAR (150)  NULL,
    [source_system]          VARCHAR (150)  NULL,
    [source_table_name]      VARCHAR (250)  NULL,
    [udw_stage_table_schema] NVARCHAR (128) NULL,
    [udw_stage_table_name]   NVARCHAR (128) NULL,
    [udw_table_schema]       [sysname]      NULL,
    [udw_table_name]         [sysname]      NULL,
    [view_schema]            NVARCHAR (128) NULL,
    [view_name]              [sysname]      NULL,
    [TopViewSchema]          NVARCHAR (128) NULL,
    [TopViewName]            [sysname]      NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

