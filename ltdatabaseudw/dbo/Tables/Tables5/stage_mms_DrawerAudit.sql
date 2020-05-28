CREATE TABLE [dbo].[stage_mms_DrawerAudit] (
    [stage_mms_DrawerAudit_id] BIGINT          NOT NULL,
    [DrawerAuditID]            INT             NULL,
    [EmployeeOneID]            INT             NULL,
    [DrawerActivityID]         INT             NULL,
    [Amount]                   DECIMAL (26, 6) NULL,
    [AuditDateTime]            DATETIME        NULL,
    [ValDrawerAuditTypeID]     TINYINT         NULL,
    [UTCAuditDateTime]         DATETIME        NULL,
    [AuditDateTimeZone]        VARCHAR (4)     NULL,
    [InsertedDateTime]         DATETIME        NULL,
    [UpdatedDateTime]          DATETIME        NULL,
    [ValPaymentTypeID]         TINYINT         NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

