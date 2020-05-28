CREATE TABLE [dbo].[stage_hash_nmo_hubtaskaudit] (
    [stage_hash_nmo_hubtaskaudit_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)       NOT NULL,
    [id]                             INT             NULL,
    [hubtaskid]                      INT             NULL,
    [operation]                      NVARCHAR (400)  NULL,
    [field]                          NVARCHAR (4000) NULL,
    [oldvalue]                       NVARCHAR (4000) NULL,
    [newvalue]                       NVARCHAR (4000) NULL,
    [modifiedpartyid]                INT             NULL,
    [modifiedname]                   NVARCHAR (60)   NULL,
    [createddate]                    DATETIME        NULL,
    [updateddate]                    DATETIME        NULL,
    [dv_load_date_time]              DATETIME        NOT NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

