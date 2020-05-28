CREATE TABLE [dbo].[dim_reporting_hierarchy_history] (
    [dim_reporting_hierarchy_history_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_reporting_hierarchy_key]        CHAR (32)     NULL,
    [effective_dim_date_key]             CHAR (8)      NULL,
    [expiration_dim_date_key]            CHAR (8)      NULL,
    [reporting_department]               VARCHAR (500) NULL,
    [reporting_division]                 VARCHAR (500) NULL,
    [reporting_product_group]            VARCHAR (500) NULL,
    [reporting_region_type]              VARCHAR (500) NULL,
    [reporting_sub_division]             VARCHAR (500) NULL,
    [dv_load_date_time]                  DATETIME      NULL,
    [dv_load_end_date_time]              DATETIME      NULL,
    [dv_batch_id]                        BIGINT        NOT NULL,
    [dv_inserted_date_time]              DATETIME      NOT NULL,
    [dv_insert_user]                     VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]               DATETIME      NULL,
    [dv_update_user]                     VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

