CREATE TABLE [dbo].[s_boss_asi_resource] (
    [s_boss_asi_resource_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [resource_type]          CHAR (25)    NULL,
    [interval_len]           SMALLINT     NULL,
    [start_time]             DATETIME     NULL,
    [end_time]               DATETIME     NULL,
    [availability]           CHAR (1)     NULL,
    [dept_affinity]          SMALLINT     NULL,
    [cancel_notify]          INT          NULL,
    [interest_affinity]      SMALLINT     NULL,
    [resource_type_id]       SMALLINT     NULL,
    [advance_days]           SMALLINT     NULL,
    [min_slots]              INT          NULL,
    [max_slots]              INT          NULL,
    [web_slots_int_mult]     INT          NULL,
    [web_active]             CHAR (1)     NULL,
    [dv_load_date_time]      DATETIME     NOT NULL,
    [dv_r_load_source_id]    BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL,
    [dv_hash]                CHAR (32)    NOT NULL,
    [dv_batch_id]            BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_resource]([dv_batch_id] ASC);

