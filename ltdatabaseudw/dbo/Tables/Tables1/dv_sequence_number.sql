CREATE TABLE [dbo].[dv_sequence_number] (
    [dv_sequence_number_id] BIGINT        NULL,
    [table_name]            VARCHAR (256) NULL,
    [max_sequence_number]   BIGINT        NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

