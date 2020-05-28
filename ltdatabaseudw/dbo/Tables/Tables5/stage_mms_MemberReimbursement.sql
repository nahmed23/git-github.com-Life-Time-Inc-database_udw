CREATE TABLE [dbo].[stage_mms_MemberReimbursement] (
    [stage_mms_MemberReimbursement_id]       BIGINT   NOT NULL,
    [MemberReimbursementID]                  INT      NULL,
    [EnrollmentDate]                         DATETIME NULL,
    [TerminationDate]                        DATETIME NULL,
    [ReimbursementProgramID]                 INT      NULL,
    [MemberID]                               INT      NULL,
    [ValReimbursementTerminationReasonID]    INT      NULL,
    [InsertedDateTime]                       DATETIME NULL,
    [UpdatedDateTime]                        DATETIME NULL,
    [ReimbursementProgramIdentifierFormatID] INT      NULL,
    [dv_batch_id]                            BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

