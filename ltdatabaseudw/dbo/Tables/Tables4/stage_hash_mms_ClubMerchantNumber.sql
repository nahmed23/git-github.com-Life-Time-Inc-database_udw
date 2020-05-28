CREATE TABLE [dbo].[stage_hash_mms_ClubMerchantNumber] (
    [stage_hash_mms_ClubMerchantNumber_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [ClubMerchantNumberID]                 INT          NULL,
    [ClubID]                               INT          NULL,
    [MerchantNumber]                       BIGINT       NULL,
    [Description]                          VARCHAR (50) NULL,
    [ValBusinessAreaID]                    SMALLINT     NULL,
    [InsertedDateTime]                     DATETIME     NULL,
    [UpdatedDateTime]                      DATETIME     NULL,
    [MerchantLocationNumber]               VARCHAR (18) NULL,
    [AutoReconcileFlag]                    BIT          NULL,
    [ValCurrencyCodeID]                    TINYINT      NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

