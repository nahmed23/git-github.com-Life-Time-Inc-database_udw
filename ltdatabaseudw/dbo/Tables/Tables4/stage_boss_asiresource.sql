CREATE TABLE [dbo].[stage_boss_asiresource] (
    [stage_boss_asiresource_id] BIGINT    NOT NULL,
    [resource_type]             CHAR (25) NULL,
    [interval_len]              SMALLINT  NULL,
    [start_time]                DATETIME  NULL,
    [end_time]                  DATETIME  NULL,
    [default_upccode]           CHAR (15) NULL,
    [availability]              CHAR (1)  NULL,
    [dept_affinity]             SMALLINT  NULL,
    [cancel_notify]             INT       NULL,
    [interest_affinity]         SMALLINT  NULL,
    [resource_type_id]          SMALLINT  NULL,
    [advance_days]              SMALLINT  NULL,
    [min_slots]                 INT       NULL,
    [max_slots]                 INT       NULL,
    [web_slots_int_mult]        INT       NULL,
    [web_active]                CHAR (1)  NULL,
    [dv_batch_id]               BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

