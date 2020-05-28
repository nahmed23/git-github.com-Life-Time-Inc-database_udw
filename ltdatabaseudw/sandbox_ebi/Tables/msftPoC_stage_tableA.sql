CREATE TABLE [sandbox_ebi].[msftPoC_stage_tableA] (
    [ID]        BIGINT      NULL,
    [ShortDesc] VARCHAR (7) NOT NULL,
    [NumVal]    BIGINT      NULL,
    [batch_id]  INT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([ID]));

