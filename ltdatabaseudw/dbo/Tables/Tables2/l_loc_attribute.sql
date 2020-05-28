CREATE TABLE [dbo].[l_loc_attribute] (
    [l_loc_attribute_id]             BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [attribute_id]                   BIGINT       NULL,
    [val_attribute_type_id]          BIGINT       NULL,
    [udw_dim_location_attribute_key] VARCHAR (32) NULL,
    [udw_business_key]               VARCHAR (32) NULL,
    [location_id]                    BIGINT       NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_hash]                        CHAR (32)    NOT NULL,
    [dv_deleted]                     BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

