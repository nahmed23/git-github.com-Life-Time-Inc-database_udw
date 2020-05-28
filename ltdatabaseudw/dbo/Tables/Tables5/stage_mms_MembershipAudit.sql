CREATE TABLE [dbo].[stage_mms_MembershipAudit] (
    [stage_mms_MembershipAudit_id] BIGINT         NOT NULL,
    [MembershipAuditId]            INT            NULL,
    [RowId]                        INT            NULL,
    [Operation]                    VARCHAR (10)   NULL,
    [ModifiedDateTime]             DATETIME       NULL,
    [ModifiedUser]                 NVARCHAR (50)  NULL,
    [ColumnName]                   VARCHAR (50)   NULL,
    [OldValue]                     VARCHAR (1000) NULL,
    [NewValue]                     VARCHAR (1000) NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

