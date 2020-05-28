CREATE TABLE [dbo].[stage_sandbox_ProductMapping] (
    [stage_sandbox_ProductMapping_id] BIGINT        NOT NULL,
    [ProductID]                       BIGINT        NULL,
    [Store_Number]                    INT           NULL,
    [ProductName]                     VARCHAR (100) NULL,
    [Category]                        VARCHAR (25)  NULL,
    [Segment]                         VARCHAR (25)  NULL,
    [BackBar]                         BIT           NULL,
    [UpdatedDateTime]                 DATETIME      NULL,
    [CommissionMapping]               VARCHAR (25)  NULL,
    [jan_one]                         DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

