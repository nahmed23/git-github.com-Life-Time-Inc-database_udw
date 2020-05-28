CREATE TABLE [dbo].[stage_mms_ReimbursementProgramIdentifierFormatPart] (
    [stage_mms_ReimbursementProgramIdentifierFormatPart_id] BIGINT        NOT NULL,
    [ReimbursementProgramIdentifierFormatPartID]            INT           NULL,
    [ReimbursementProgramIdentifierFormatID]                INT           NULL,
    [FieldName]                                             VARCHAR (50)  NULL,
    [FieldSize]                                             SMALLINT      NULL,
    [FieldValidationRule]                                   VARCHAR (500) NULL,
    [FieldValidationErrorMessage]                           VARCHAR (255) NULL,
    [FieldSequence]                                         INT           NULL,
    [InsertedDateTime]                                      DATETIME      NULL,
    [UpdatedDateTime]                                       DATETIME      NULL,
    [dv_batch_id]                                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

