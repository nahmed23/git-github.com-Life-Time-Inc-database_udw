CREATE TABLE [dbo].[dv_etl_map_backup_identity] (
    [dv_etl_map_id]                     BIGINT         IDENTITY (1, 1) NOT NULL,
    [dv_table]                          VARCHAR (500)  NOT NULL,
    [dv_column]                         VARCHAR (500)  NOT NULL,
    [source]                            VARCHAR (500)  NOT NULL,
    [source_table]                      VARCHAR (500)  NOT NULL,
    [source_column]                     VARCHAR (500)  NOT NULL,
    [data_type]                         VARCHAR (500)  NOT NULL,
    [sort_order]                        INT            NOT NULL,
    [comments]                          VARCHAR (8000) NOT NULL,
    [release]                           VARCHAR (500)  NOT NULL,
    [partition_scheme]                  VARCHAR (100)  NULL,
    [is_truncated_staging]              BIT            NOT NULL,
    [greatest_satellite_date_time_type] CHAR (1)       NULL,
    [business_key_sort_order]           INT            NULL,
    [historical_pit_flag]               INT            DEFAULT ((0)) NOT NULL,
    [distribution_column]               VARCHAR (100)  NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL,
    [dv_batch_id]                       BIGINT         NOT NULL,
    [dv_backup_date_time]               DATETIME       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

