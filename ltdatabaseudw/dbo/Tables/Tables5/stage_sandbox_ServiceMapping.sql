CREATE TABLE [dbo].[stage_sandbox_ServiceMapping] (
    [stage_sandbox_ServiceMapping_id] BIGINT        NOT NULL,
    [ServiceID]                       BIGINT        NULL,
    [Store_Number]                    INT           NULL,
    [ServiceName]                     VARCHAR (100) NULL,
    [Category]                        VARCHAR (25)  NULL,
    [Segment]                         VARCHAR (25)  NULL,
    [UpdatedDateTime]                 DATETIME      NULL,
    [CommissionMapping]               VARCHAR (25)  NULL,
    [jan_one]                         DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

