CREATE TABLE [dbo].[dim_mms_merchant_number] (
    [dim_mms_merchant_number_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [auto_reconcile_flag]               CHAR (1)     NULL,
    [business_area_dim_description_key] CHAR (255)   NULL,
    [club_id]                           BIGINT       NULL,
    [club_merchant_number_id]           INT          NULL,
    [currency_code]                     CHAR (3)     NULL,
    [dim_club_key]                      CHAR (32)    NULL,
    [dim_mms_merchant_number_key]       CHAR (32)    NULL,
    [merchant_location_number]          VARCHAR (18) NULL,
    [merchant_number]                   BIGINT       NULL,
    [val_business_area_id]              INT          NULL,
    [dv_load_date_time]                 DATETIME     NULL,
    [dv_load_end_date_time]             DATETIME     NULL,
    [dv_batch_id]                       BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_mms_merchant_number_key]));

