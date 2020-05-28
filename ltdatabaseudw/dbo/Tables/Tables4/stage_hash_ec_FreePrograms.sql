CREATE TABLE [dbo].[stage_hash_ec_FreePrograms] (
    [stage_hash_ec_FreePrograms_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [ProgramId]                     INT             NULL,
    [ProgramImage]                  NVARCHAR (2000) NULL,
    [ProgramName]                   NVARCHAR (50)   NULL,
    [ProgramDescription]            NVARCHAR (4000) NULL,
    [Featured]                      BIT             NULL,
    [Priority]                      INT             NULL,
    [Frequency]                     NVARCHAR (50)   NULL,
    [Duration]                      NVARCHAR (50)   NULL,
    [Equipment]                     NVARCHAR (1000) NULL,
    [Exercise]                      NVARCHAR (1000) NULL,
    [Level]                         NVARCHAR (50)   NULL,
    [Goal]                          NVARCHAR (50)   NULL,
    [IsActive]                      BIT             NULL,
    [CreatedDate]                   DATETIME        NULL,
    [UpdatedDate]                   DATETIME        NULL,
    [FreeProgramId]                 INT             NULL,
    [EndDate]                       DATETIME        NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

