CREATE TABLE [dbo].[stage_hash_mms_DrawerActivityAmount] (
    [stage_hash_mms_DrawerActivityAmount_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [DrawerActivityAmountID]                 INT             NULL,
    [DrawerActivityID]                       INT             NULL,
    [TranTotalAmount]                        DECIMAL (26, 6) NULL,
    [ActualTotalAmount]                      DECIMAL (26, 6) NULL,
    [ValPaymentTypeID]                       TINYINT         NULL,
    [InsertedDateTime]                       DATETIME        NULL,
    [UpdatedDateTime]                        DATETIME        NULL,
    [ValCurrencyCodeID]                      TINYINT         NULL,
    [dv_load_date_time]                      DATETIME        NOT NULL,
    [dv_inserted_date_time]                  DATETIME        NOT NULL,
    [dv_insert_user]                         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL,
    [dv_batch_id]                            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

