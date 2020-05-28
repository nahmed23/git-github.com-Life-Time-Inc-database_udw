CREATE TABLE [dbo].[stage_mms_ClubProductTaxRate] (
    [stage_mms_ClubProductTaxRate_id] BIGINT   NOT NULL,
    [ClubProductTaxRateID]            INT      NULL,
    [ClubID]                          INT      NULL,
    [ProductID]                       INT      NULL,
    [TaxRateID]                       SMALLINT NULL,
    [StartDate]                       DATETIME NULL,
    [EndDate]                         DATETIME NULL,
    [InsertedDatetime]                DATETIME NULL,
    [UpdatedDateTime]                 DATETIME NULL,
    [dv_batch_id]                     BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

