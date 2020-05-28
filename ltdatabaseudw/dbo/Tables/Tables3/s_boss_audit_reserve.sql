CREATE TABLE [dbo].[s_boss_audit_reserve] (
    [s_boss_audit_reserve_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)    NOT NULL,
    [audit_reserve_id]        INT          NULL,
    [club]                    INT          NULL,
    [start_date]              DATETIME     NULL,
    [create_date]             DATETIME     NULL,
    [upc_desc]                VARCHAR (50) NULL,
    [reservation_type]        CHAR (1)     NULL,
    [audit_type]              VARCHAR (50) NULL,
    [audit_performed]         DATETIME     NULL,
    [dv_load_date_time]       DATETIME     NOT NULL,
    [dv_r_load_source_id]     BIGINT       NOT NULL,
    [dv_inserted_date_time]   DATETIME     NOT NULL,
    [dv_insert_user]          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]    DATETIME     NULL,
    [dv_update_user]          VARCHAR (50) NULL,
    [dv_hash]                 CHAR (32)    NOT NULL,
    [dv_deleted]              BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_audit_reserve]([dv_batch_id] ASC);

