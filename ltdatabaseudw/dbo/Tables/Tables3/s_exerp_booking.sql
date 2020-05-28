CREATE TABLE [dbo].[s_exerp_booking] (
    [s_exerp_booking_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)      NOT NULL,
    [booking_id]            VARCHAR (4000) NULL,
    [name]                  VARCHAR (4000) NULL,
    [color]                 VARCHAR (4000) NULL,
    [start_datetime]        DATETIME       NULL,
    [stop_datetime]         DATETIME       NULL,
    [creation_datetime]     DATETIME       NULL,
    [state]                 VARCHAR (4000) NULL,
    [ets]                   BIGINT         NULL,
    [class_capacity]        INT            NULL,
    [waiting_list_capacity] INT            NULL,
    [cancel_datetime]       DATETIME       NULL,
    [cancel_reason]         VARCHAR (4000) NULL,
    [max_capacity_override] INT            NULL,
    [description]           VARCHAR (4000) NULL,
    [comment]               VARCHAR (4000) NULL,
    [dv_load_date_time]     DATETIME       NOT NULL,
    [dv_r_load_source_id]   BIGINT         NOT NULL,
    [dv_inserted_date_time] DATETIME       NOT NULL,
    [dv_insert_user]        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]  DATETIME       NULL,
    [dv_update_user]        VARCHAR (50)   NULL,
    [dv_hash]               CHAR (32)      NOT NULL,
    [dv_deleted]            BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_booking]([dv_batch_id] ASC);

