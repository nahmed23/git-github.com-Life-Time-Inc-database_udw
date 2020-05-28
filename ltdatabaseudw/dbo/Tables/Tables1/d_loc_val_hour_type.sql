CREATE TABLE [dbo].[d_loc_val_hour_type] (
    [d_loc_val_hour_type_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [val_hour_type_id]                  BIGINT         NULL,
    [d_loc_val_hour_type_group_bk_hash] VARCHAR (32)   NULL,
    [display_name]                      VARCHAR (4000) NULL,
    [val_hour_type_name]                VARCHAR (100)  NULL,
    [p_loc_val_hour_type_id]            BIGINT         NOT NULL,
    [deleted_flag]                      INT            NULL,
    [dv_load_date_time]                 DATETIME       NULL,
    [dv_load_end_date_time]             DATETIME       NULL,
    [dv_batch_id]                       BIGINT         NOT NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

