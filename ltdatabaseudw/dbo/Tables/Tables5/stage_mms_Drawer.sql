CREATE TABLE [dbo].[stage_mms_Drawer] (
    [stage_mms_Drawer_id] BIGINT          NOT NULL,
    [DrawerID]            INT             NULL,
    [ClubID]              INT             NULL,
    [LockedFlag]          BIT             NULL,
    [Description]         VARCHAR (50)    NULL,
    [InsertedDateTime]    DATETIME        NULL,
    [UpdatedDateTime]     DATETIME        NULL,
    [StartingCashAmount]  DECIMAL (26, 6) NULL,
    [dv_batch_id]         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

