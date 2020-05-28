CREATE TABLE [dbo].[ltf_system_et_execution] (
    [executionid] BIGINT   NULL,
    [windowstart] DATETIME NULL,
    [windowend]   DATETIME NULL,
    [jobstart]    DATETIME NULL,
    [jobend]      DATETIME NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

