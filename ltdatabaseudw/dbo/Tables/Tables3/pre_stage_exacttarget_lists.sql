CREATE TABLE [dbo].[pre_stage_exacttarget_lists] (
    [clientid]    BIGINT        NULL,
    [listid]      BIGINT        NULL,
    [name]        VARCHAR (100) NULL,
    [description] VARCHAR (100) NULL,
    [datecreated] DATETIME      NULL,
    [status]      VARCHAR (20)  NULL,
    [listtype]    VARCHAR (20)  NULL,
    [executionid] BIGINT        NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

