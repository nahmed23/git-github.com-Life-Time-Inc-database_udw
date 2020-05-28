﻿CREATE TABLE [dbo].[dv_d_etl_map_backup_nonidentity] (
    [dv_d_etl_map_id]       BIGINT         NOT NULL,
    [target_object]         VARCHAR (500)  NOT NULL,
    [target_column]         VARCHAR (500)  NOT NULL,
    [data_type]             VARCHAR (500)  NOT NULL,
    [source_sql]            VARCHAR (8000) NULL,
    [partition_scheme]      VARCHAR (100)  NULL,
    [release]               VARCHAR (500)  NOT NULL,
    [dv_inserted_date_time] DATETIME       NOT NULL,
    [dv_insert_user]        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]  DATETIME       NULL,
    [dv_update_user]        VARCHAR (50)   NULL,
    [dv_batch_id]           BIGINT         NOT NULL,
    [dv_backup_date_time]   DATETIME       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

