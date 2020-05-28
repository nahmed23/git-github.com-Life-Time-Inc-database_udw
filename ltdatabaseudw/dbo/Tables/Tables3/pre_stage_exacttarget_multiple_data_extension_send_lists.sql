CREATE TABLE [dbo].[pre_stage_exacttarget_multiple_data_extension_send_lists] (
    [clientid]          BIGINT        NULL,
    [sendid]            BIGINT        NULL,
    [listid]            BIGINT        NULL,
    [dataextensionname] VARCHAR (100) NULL,
    [status]            VARCHAR (20)  NULL,
    [datecreated]       DATETIME      NULL,
    [declientid]        BIGINT        NULL,
    [executionid]       BIGINT        NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

