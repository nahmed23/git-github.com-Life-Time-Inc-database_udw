CREATE TABLE [dbo].[r_mms_val_time_zone] (
    [r_mms_val_time_zone_id] BIGINT       NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [val_time_zone_id]       INT          NULL,
    [description]            VARCHAR (50) NULL,
    [sort_order]             INT          NULL,
    [inserted_date_time]     DATETIME     NULL,
    [abbreviation]           VARCHAR (25) NULL,
    [updated_date_time]      DATETIME     NULL,
    [dst_offset]             INT          NULL,
    [st_offset]              INT          NULL,
    [dv_load_date_time]      DATETIME     NOT NULL,
    [dv_load_end_date_time]  DATETIME     NOT NULL,
    [dv_batch_id]            BIGINT       NOT NULL,
    [dv_r_load_source_id]    BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL,
    [dv_hash]                CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([r_mms_val_time_zone_id]));

