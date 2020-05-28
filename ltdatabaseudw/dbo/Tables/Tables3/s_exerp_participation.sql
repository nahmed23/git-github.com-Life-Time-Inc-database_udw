CREATE TABLE [dbo].[s_exerp_participation] (
    [s_exerp_participation_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [participation_id]         VARCHAR (4000) NULL,
    [creation_datetime]        DATETIME       NULL,
    [state]                    VARCHAR (4000) NULL,
    [user_interface_type]      VARCHAR (4000) NULL,
    [show_up_datetime]         DATETIME       NULL,
    [show_up_interface_type]   VARCHAR (4000) NULL,
    [show_up_using_card]       BIT            NULL,
    [cancel_datetime]          DATETIME       NULL,
    [cancel_interface_type]    VARCHAR (4000) NULL,
    [cancel_reason]            VARCHAR (4000) NULL,
    [was_on_waiting_list]      BIT            NULL,
    [ets]                      BIGINT         NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_r_load_source_id]      BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_hash]                  CHAR (32)      NOT NULL,
    [dv_deleted]               BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_participation]([dv_batch_id] ASC);

