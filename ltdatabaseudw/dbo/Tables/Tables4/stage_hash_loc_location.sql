CREATE TABLE [dbo].[stage_hash_loc_location] (
    [stage_hash_loc_location_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [location_id]                BIGINT         NULL,
    [udw_business_key]           VARCHAR (32)   NULL,
    [val_location_type_id]       BIGINT         NULL,
    [udw_dim_location_key]       VARCHAR (32)   NULL,
    [description]                VARCHAR (4000) NULL,
    [display_name]               VARCHAR (4000) NULL,
    [top_level_location_id]      BIGINT         NULL,
    [udw_source_name]            VARCHAR (100)  NULL,
    [parent_location_id]         BIGINT         NULL,
    [hierarchy_level]            BIGINT         NULL,
    [created_date_time]          DATETIME       NULL,
    [created_by]                 VARCHAR (100)  NULL,
    [deleted_date_time]          DATETIME       NULL,
    [deleted_by]                 VARCHAR (100)  NULL,
    [last_updated_date_time]     DATETIME       NULL,
    [last_updated_by]            VARCHAR (100)  NULL,
    [managed_by_udw_flag]        CHAR (1)       NULL,
    [slug]                       VARCHAR (100)  NULL,
    [external_id]                INT            NULL,
    [dv_load_date_time]          DATETIME       NOT NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

