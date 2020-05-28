CREATE TABLE [dbo].[stage_mms_ValIGProfitCenter] (
    [stage_mms_ValIGProfitCenter_id] BIGINT       NOT NULL,
    [ValIGProfitCenterID]            INT          NULL,
    [Description]                    VARCHAR (50) NULL,
    [ProfitCenterNumber]             INT          NULL,
    [SortOrder]                      INT          NULL,
    [InsertedDateTime]               DATETIME     NULL,
    [UpdatedDateTime]                DATETIME     NULL,
    [ClubID]                         INT          NULL,
    [AutoReconcileTipsFlag]          BIT          NULL,
    [ValProductSalesChannelID]       INT          NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

