CREATE TABLE [dbo].[stage_mms_ValRecurrentProductType] (
    [stage_mms_ValRecurrentProductType_id] BIGINT       NOT NULL,
    [ValRecurrentProductTypeID]            INT          NULL,
    [Description]                          VARCHAR (50) NULL,
    [SortOrder]                            INT          NULL,
    [InsertedDateTime]                     DATETIME     NULL,
    [UpdatedDateTime]                      DATETIME     NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

