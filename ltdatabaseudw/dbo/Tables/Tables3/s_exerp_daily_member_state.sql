CREATE TABLE [dbo].[s_exerp_daily_member_state] (
    [s_exerp_daily_member_state_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [daily_member_state_id]         INT            NULL,
    [date]                          DATETIME       NULL,
    [entry_datetime]                DATETIME       NULL,
    [change]                        VARCHAR (4000) NULL,
    [member_number_delta]           INT            NULL,
    [extra_number_delta]            INT            NULL,
    [secondary_member_number_delta] INT            NULL,
    [cancel_datetime]               DATETIME       NULL,
    [ets]                           BIGINT         NULL,
    [dummy_modified_date_time]      DATETIME       NULL,
    [dv_load_date_time]             DATETIME       NOT NULL,
    [dv_r_load_source_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL,
    [dv_hash]                       CHAR (32)      NOT NULL,
    [dv_deleted]                    BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_daily_member_state]([dv_batch_id] ASC);

