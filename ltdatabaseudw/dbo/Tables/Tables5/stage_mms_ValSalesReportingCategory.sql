CREATE TABLE [dbo].[stage_mms_ValSalesReportingCategory] (
    [stage_mms_ValSalesReportingCategory_id] BIGINT       NOT NULL,
    [ValSalesReportingCategoryID]            INT          NULL,
    [Description]                            VARCHAR (50) NULL,
    [SortOrder]                              INT          NULL,
    [InsertedDateTime]                       DATETIME     NULL,
    [UpdatedDateTime]                        DATETIME     NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

