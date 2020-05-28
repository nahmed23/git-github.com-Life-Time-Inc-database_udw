CREATE TABLE [dbo].[stage_mms_ClubMerchantNumber] (
    [stage_mms_ClubMerchantNumber_id] BIGINT       NOT NULL,
    [ClubMerchantNumberID]            INT          NULL,
    [ClubID]                          INT          NULL,
    [MerchantNumber]                  BIGINT       NULL,
    [Description]                     VARCHAR (50) NULL,
    [ValBusinessAreaID]               SMALLINT     NULL,
    [InsertedDateTime]                DATETIME     NULL,
    [UpdatedDateTime]                 DATETIME     NULL,
    [MerchantLocationNumber]          VARCHAR (18) NULL,
    [AutoReconcileFlag]               BIT          NULL,
    [ValCurrencyCodeID]               TINYINT      NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

