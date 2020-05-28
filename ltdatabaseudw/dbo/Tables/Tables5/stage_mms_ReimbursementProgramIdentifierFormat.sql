CREATE TABLE [dbo].[stage_mms_ReimbursementProgramIdentifierFormat] (
    [stage_mms_ReimbursementProgramIdentifierFormat_id] BIGINT        NOT NULL,
    [ReimbursementProgramIdentifierFormatID]            INT           NULL,
    [ReimbursementProgramID]                            INT           NULL,
    [Description]                                       VARCHAR (50)  NULL,
    [ActiveFlag]                                        BIT           NULL,
    [InsertedDateTime]                                  DATETIME      NULL,
    [UpdatedDateTime]                                   DATETIME      NULL,
    [ImageURL]                                          VARCHAR (250) NULL,
    [ImageDescription]                                  VARCHAR (250) NULL,
    [SortOrder]                                         INT           NULL,
    [ValProgramIdentifierValidationClassID]             TINYINT       NULL,
    [dv_batch_id]                                       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

