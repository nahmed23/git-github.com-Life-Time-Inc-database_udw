CREATE TABLE [dbo].[stage_mms_TaxRate] (
    [stage_mms_TaxRate_id] BIGINT         NOT NULL,
    [TaxRateID]            SMALLINT       NULL,
    [ValTaxTypeID]         SMALLINT       NULL,
    [TaxPercentage]        DECIMAL (4, 2) NULL,
    [InsertedDatetime]     DATETIME       NULL,
    [UpdatedDateTime]      DATETIME       NULL,
    [dv_batch_id]          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

