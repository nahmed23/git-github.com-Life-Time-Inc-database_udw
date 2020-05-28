CREATE TABLE [dbo].[stage_hash_boss_auditreserve] (
    [stage_hash_boss_auditreserve_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [id]                              INT          NULL,
    [reservation]                     INT          NULL,
    [club]                            INT          NULL,
    [start_date]                      DATETIME     NULL,
    [create_date]                     DATETIME     NULL,
    [upc_desc]                        VARCHAR (50) NULL,
    [reservation_type]                CHAR (1)     NULL,
    [audit_type]                      VARCHAR (50) NULL,
    [audit_performed]                 DATETIME     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

