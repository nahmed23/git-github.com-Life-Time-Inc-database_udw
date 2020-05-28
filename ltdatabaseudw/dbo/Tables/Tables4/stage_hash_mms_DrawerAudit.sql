CREATE TABLE [dbo].[stage_hash_mms_DrawerAudit] (
    [stage_hash_mms_DrawerAudit_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [DrawerAuditID]                 INT             NULL,
    [EmployeeOneID]                 INT             NULL,
    [DrawerActivityID]              INT             NULL,
    [Amount]                        DECIMAL (26, 6) NULL,
    [AuditDateTime]                 DATETIME        NULL,
    [ValDrawerAuditTypeID]          TINYINT         NULL,
    [UTCAuditDateTime]              DATETIME        NULL,
    [AuditDateTimeZone]             VARCHAR (4)     NULL,
    [InsertedDateTime]              DATETIME        NULL,
    [UpdatedDateTime]               DATETIME        NULL,
    [ValPaymentTypeID]              TINYINT         NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_inserted_date_time]         DATETIME        NOT NULL,
    [dv_insert_user]                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]          DATETIME        NULL,
    [dv_update_user]                VARCHAR (50)    NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

