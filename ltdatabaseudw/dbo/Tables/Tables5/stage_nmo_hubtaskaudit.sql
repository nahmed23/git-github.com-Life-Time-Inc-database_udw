CREATE TABLE [dbo].[stage_nmo_hubtaskaudit] (
    [stage_nmo_hubtaskaudit_id] BIGINT          NOT NULL,
    [id]                        INT             NULL,
    [hubtaskid]                 INT             NULL,
    [operation]                 NVARCHAR (400)  NULL,
    [field]                     NVARCHAR (4000) NULL,
    [oldvalue]                  NVARCHAR (4000) NULL,
    [newvalue]                  NVARCHAR (4000) NULL,
    [modifiedpartyid]           INT             NULL,
    [modifiedname]              NVARCHAR (60)   NULL,
    [createddate]               DATETIME        NULL,
    [updateddate]               DATETIME        NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

