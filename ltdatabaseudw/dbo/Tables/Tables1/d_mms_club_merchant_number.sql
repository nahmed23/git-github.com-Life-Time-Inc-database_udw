CREATE TABLE [dbo].[d_mms_club_merchant_number] (
    [d_mms_club_merchant_number_id]     BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)     NOT NULL,
    [dim_mms_merchant_number_key]       CHAR (32)     NULL,
    [club_merchant_number_id]           INT           NULL,
    [auto_reconcile_flag]               CHAR (1)      NULL,
    [business_area_dim_description_key] VARCHAR (532) NULL,
    [currency_code]                     CHAR (32)     NULL,
    [dim_club_key]                      CHAR (32)     NULL,
    [merchant_location_number]          VARCHAR (18)  NULL,
    [merchant_number]                   BIGINT        NULL,
    [val_business_area_id]              INT           NULL,
    [val_currency_code_id]              INT           NULL,
    [p_mms_club_merchant_number_id]     BIGINT        NOT NULL,
    [deleted_flag]                      INT           NULL,
    [dv_load_date_time]                 DATETIME      NULL,
    [dv_load_end_date_time]             DATETIME      NULL,
    [dv_batch_id]                       BIGINT        NOT NULL,
    [dv_inserted_date_time]             DATETIME      NOT NULL,
    [dv_insert_user]                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]              DATETIME      NULL,
    [dv_update_user]                    VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

