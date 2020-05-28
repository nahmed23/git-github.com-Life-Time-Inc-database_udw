CREATE TABLE [dbo].[stage_mms_ClubProduct] (
    [stage_mms_ClubProduct_id] BIGINT          NOT NULL,
    [ClubProductID]            INT             NULL,
    [ClubID]                   INT             NULL,
    [ProductID]                INT             NULL,
    [Price]                    DECIMAL (26, 6) NULL,
    [ValCommissionableID]      TINYINT         NULL,
    [InsertedDateTime]         DATETIME        NULL,
    [SoldInPK]                 BIT             NULL,
    [UpdatedDateTime]          DATETIME        NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

