CREATE TABLE [dbo].[stage_mms_MemberReimbursementProgramIdentifierPart] (
    [stage_mms_MemberReimbursementProgramIdentifierPart_id] BIGINT        NOT NULL,
    [MemberReimbursementProgramIdentifierPartID]            INT           NULL,
    [MemberReimbursementID]                                 INT           NULL,
    [ReimbursementProgramIdentifierFormatPartID]            INT           NULL,
    [PartValue]                                             VARCHAR (100) NULL,
    [InsertedDateTime]                                      DATETIME      NULL,
    [UpdatedDateTime]                                       DATETIME      NULL,
    [dv_batch_id]                                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

