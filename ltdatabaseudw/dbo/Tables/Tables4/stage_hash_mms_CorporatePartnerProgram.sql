CREATE TABLE [dbo].[stage_hash_mms_CorporatePartnerProgram] (
    [stage_hash_mms_CorporatePartnerProgram_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)    NOT NULL,
    [CorporatePartnerProgramID]                 INT          NULL,
    [CorporatePartnerID]                        INT          NULL,
    [ProgramName]                               VARCHAR (50) NULL,
    [ReimbursementProgramID]                    INT          NULL,
    [ReimbursementProgramIdentifierFormatID]    INT          NULL,
    [EffectiveFromDateTime]                     DATETIME     NULL,
    [EffectiveThruDateTime]                     DATETIME     NULL,
    [InsertedDateTime]                          DATETIME     NULL,
    [UpdatedDateTime]                           DATETIME     NULL,
    [dv_load_date_time]                         DATETIME     NOT NULL,
    [dv_inserted_date_time]                     DATETIME     NOT NULL,
    [dv_insert_user]                            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                      DATETIME     NULL,
    [dv_update_user]                            VARCHAR (50) NULL,
    [dv_batch_id]                               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

