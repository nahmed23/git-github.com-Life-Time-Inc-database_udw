CREATE TABLE [dbo].[stage_mms_CorporatePartnerProgram] (
    [stage_mms_CorporatePartnerProgram_id]   BIGINT       NOT NULL,
    [CorporatePartnerProgramID]              INT          NULL,
    [CorporatePartnerID]                     INT          NULL,
    [ProgramName]                            VARCHAR (50) NULL,
    [ReimbursementProgramID]                 INT          NULL,
    [ReimbursementProgramIdentifierFormatID] INT          NULL,
    [EffectiveFromDateTime]                  DATETIME     NULL,
    [EffectiveThruDateTime]                  DATETIME     NULL,
    [InsertedDateTime]                       DATETIME     NULL,
    [UpdatedDateTime]                        DATETIME     NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

