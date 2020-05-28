CREATE TABLE [dbo].[stage_mms_ValProgramIdentifierValidationClass] (
    [stage_mms_ValProgramIdentifierValidationClass_id] BIGINT        NOT NULL,
    [ValProgramIdentifierValidationClassID]            INT           NULL,
    [Description]                                      VARCHAR (50)  NULL,
    [Class]                                            VARCHAR (250) NULL,
    [SortOrder]                                        INT           NULL,
    [InsertedDateTime]                                 DATETIME      NULL,
    [UpdatedDateTime]                                  DATETIME      NULL,
    [dv_batch_id]                                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

