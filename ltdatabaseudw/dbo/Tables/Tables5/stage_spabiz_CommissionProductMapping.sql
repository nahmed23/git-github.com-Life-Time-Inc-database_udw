CREATE TABLE [dbo].[stage_spabiz_CommissionProductMapping] (
    [stage_spabiz_CommissionProductMapping_id] BIGINT         NOT NULL,
    [ProductName]                              VARCHAR (4000) NULL,
    [MappingGroupName]                         VARCHAR (4000) NULL,
    [ProductMappingType]                       VARCHAR (4000) NULL,
    [jan_one]                                  DATETIME       NULL,
    [dv_batch_id]                              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

