CREATE TABLE [dbo].[stage_mms_ReimbursementProgram] (
    [stage_mms_ReimbursementProgram_id]       BIGINT         NOT NULL,
    [ReimbursementProgramID]                  INT            NULL,
    [ReimbursementProgramName]                VARCHAR (50)   NULL,
    [ActiveFlag]                              BIT            NULL,
    [InsertedDateTime]                        DATETIME       NULL,
    [UpdatedDateTime]                         DATETIME       NULL,
    [DuesSubsidyAmount]                       DECIMAL (6, 2) NULL,
    [CompanyID]                               INT            NULL,
    [ValReimbursementProgramProcessingTypeID] TINYINT        NULL,
    [ValReimbursementProgramTypeID]           TINYINT        NULL,
    [dv_batch_id]                             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

