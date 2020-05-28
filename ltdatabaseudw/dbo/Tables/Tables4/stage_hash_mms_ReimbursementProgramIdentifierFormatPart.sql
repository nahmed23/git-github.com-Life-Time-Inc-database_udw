CREATE TABLE [dbo].[stage_hash_mms_ReimbursementProgramIdentifierFormatPart] (
    [stage_hash_mms_ReimbursementProgramIdentifierFormatPart_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                    CHAR (32)     NOT NULL,
    [ReimbursementProgramIdentifierFormatPartID]                 INT           NULL,
    [ReimbursementProgramIdentifierFormatID]                     INT           NULL,
    [FieldName]                                                  VARCHAR (50)  NULL,
    [FieldSize]                                                  SMALLINT      NULL,
    [FieldValidationRule]                                        VARCHAR (500) NULL,
    [FieldValidationErrorMessage]                                VARCHAR (255) NULL,
    [FieldSequence]                                              INT           NULL,
    [InsertedDateTime]                                           DATETIME      NULL,
    [UpdatedDateTime]                                            DATETIME      NULL,
    [dv_load_date_time]                                          DATETIME      NOT NULL,
    [dv_inserted_date_time]                                      DATETIME      NOT NULL,
    [dv_insert_user]                                             VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                                       DATETIME      NULL,
    [dv_update_user]                                             VARCHAR (50)  NULL,
    [dv_batch_id]                                                BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

