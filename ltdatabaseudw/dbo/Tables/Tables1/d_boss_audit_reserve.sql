CREATE TABLE [dbo].[d_boss_audit_reserve] (
    [d_boss_audit_reserve_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [dim_boss_audit_reserve_key] CHAR (32)    NULL,
    [audit_reserve_id]           INT          NULL,
    [audit_performed]            DATETIME     NULL,
    [audit_type]                 VARCHAR (50) NULL,
    [created_date_key]           INT          NULL,
    [created_time_key]           INT          NULL,
    [dim_boss_reservation_key]   VARCHAR (36) NULL,
    [dim_club_key]               CHAR (32)    NULL,
    [reservation_type]           CHAR (1)     NULL,
    [start_date_time]            DATETIME     NULL,
    [upc_desc]                   VARCHAR (50) NULL,
    [p_boss_audit_reserve_id]    BIGINT       NOT NULL,
    [deleted_flag]               INT          NULL,
    [dv_load_date_time]          DATETIME     NULL,
    [dv_load_end_date_time]      DATETIME     NULL,
    [dv_batch_id]                BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_audit_reserve]([dv_batch_id] ASC);

