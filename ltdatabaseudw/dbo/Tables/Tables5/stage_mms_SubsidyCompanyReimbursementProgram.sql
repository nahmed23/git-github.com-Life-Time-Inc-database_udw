CREATE TABLE [dbo].[stage_mms_SubsidyCompanyReimbursementProgram] (
    [stage_mms_SubsidyCompanyReimbursementProgram_id] BIGINT         NOT NULL,
    [SubsidyCompanyReimbursementProgramID]            INT            NULL,
    [SubsidyCompanyID]                                INT            NULL,
    [ReimbursementProgramID]                          INT            NULL,
    [Description]                                     VARCHAR (255)  NULL,
    [SendQualificationDataFlag]                       BIT            NULL,
    [LTFCalcFlag]                                     BIT            NULL,
    [BatchNumber]                                     INT            NULL,
    [MaximumReimbursement]                            NUMERIC (6, 2) NULL,
    [EffectiveFromDateTime]                           DATETIME       NULL,
    [EffectiveThruDateTime]                           DATETIME       NULL,
    [InsertedDateTime]                                DATETIME       NULL,
    [UpdatedDateTime]                                 DATETIME       NULL,
    [dv_batch_id]                                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

