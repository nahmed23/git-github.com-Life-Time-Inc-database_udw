CREATE TABLE [dbo].[stage_mms_ValDiscountCombineRule] (
    [stage_mms_ValDiscountCombineRule_id] BIGINT       NOT NULL,
    [ValDiscountCombineRuleID]            INT          NULL,
    [Description]                         VARCHAR (50) NULL,
    [SortOrder]                           INT          NULL,
    [InsertedDateTime]                    DATETIME     NULL,
    [UpdatedDateTime]                     DATETIME     NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

