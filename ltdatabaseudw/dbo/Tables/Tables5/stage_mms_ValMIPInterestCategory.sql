CREATE TABLE [dbo].[stage_mms_ValMIPInterestCategory] (
    [stage_mms_ValMIPInterestCategory_id] BIGINT       NOT NULL,
    [ValMIPInterestCategoryID]            SMALLINT     NULL,
    [Description]                         VARCHAR (50) NULL,
    [SortOrder]                           SMALLINT     NULL,
    [InsertedDateTime]                    DATETIME     NULL,
    [UpdatedDateTime]                     DATETIME     NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

