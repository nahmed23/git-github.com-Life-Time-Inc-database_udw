CREATE TABLE [sandbox_ebi].[msftPoC_tableD] (
    [ID]                      BIGINT      NULL,
    [ShortDesc]               VARCHAR (7) NOT NULL,
    [CalcVal]                 BIGINT      NULL,
    [batch_id]                INT         NOT NULL,
    [di_LastModifiedDateTime] DATETIME    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([ID]));

