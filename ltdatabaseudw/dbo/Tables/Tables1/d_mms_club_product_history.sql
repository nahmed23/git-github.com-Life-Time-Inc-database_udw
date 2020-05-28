CREATE TABLE [dbo].[d_mms_club_product_history] (
    [d_mms_club_product_history_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [dim_club_bridge_dim_mms_product_key] VARCHAR (32)    NULL,
    [club_product_id]                     INT             NULL,
    [effective_date_time]                 DATETIME        NULL,
    [expiration_date_time]                DATETIME        NULL,
    [dim_club_key]                        VARCHAR (32)    NULL,
    [dim_mms_product_key]                 VARCHAR (32)    NULL,
    [price]                               DECIMAL (26, 6) NULL,
    [sold_in_pk_flag]                     VARCHAR (32)    NULL,
    [val_commissionable_id]               TINYINT         NULL,
    [p_mms_club_product_id]               BIGINT          NOT NULL,
    [deleted_flag]                        INT             NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

