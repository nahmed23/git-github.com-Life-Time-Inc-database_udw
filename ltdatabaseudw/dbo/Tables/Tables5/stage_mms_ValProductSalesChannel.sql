CREATE TABLE [dbo].[stage_mms_ValProductSalesChannel] (
    [stage_mms_ValProductSalesChannel_id] BIGINT       NOT NULL,
    [ValProductSalesChannelID]            INT          NULL,
    [Description]                         VARCHAR (50) NULL,
    [SortOrder]                           INT          NULL,
    [InsertedDateTime]                    DATETIME     NULL,
    [UpdatedDateTime]                     DATETIME     NULL,
    [DisplayTerminalAdminUIFlag]          BIT          NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

