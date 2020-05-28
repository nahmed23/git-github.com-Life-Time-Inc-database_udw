CREATE TABLE [dbo].[stage_hash_mms_MemberReimbursement] (
    [stage_hash_mms_MemberReimbursement_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)    NOT NULL,
    [MemberReimbursementID]                  INT          NULL,
    [EnrollmentDate]                         DATETIME     NULL,
    [TerminationDate]                        DATETIME     NULL,
    [ReimbursementProgramID]                 INT          NULL,
    [MemberID]                               INT          NULL,
    [ValReimbursementTerminationReasonID]    INT          NULL,
    [InsertedDateTime]                       DATETIME     NULL,
    [UpdatedDateTime]                        DATETIME     NULL,
    [ReimbursementProgramIdentifierFormatID] INT          NULL,
    [dv_load_date_time]                      DATETIME     NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

