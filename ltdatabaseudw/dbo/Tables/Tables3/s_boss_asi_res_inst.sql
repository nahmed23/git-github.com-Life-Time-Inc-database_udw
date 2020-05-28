CREATE TABLE [dbo].[s_boss_asi_res_inst] (
    [s_boss_asi_res_inst_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [start_date]             DATETIME        NULL,
    [end_date]               DATETIME        NULL,
    [name]                   CHAR (30)       NULL,
    [comment]                VARCHAR (80)    NULL,
    [cost]                   DECIMAL (26, 6) NULL,
    [substitute]             CHAR (1)        NULL,
    [sub_for]                CHAR (10)       NULL,
    [asi_res_inst_id]        INT             NULL,
    [updated_at]             DATETIME        NULL,
    [created_at]             DATETIME        NULL,
    [res_color]              INT             NULL,
    [use_for_lt_bucks]       CHAR (1)        NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_r_load_source_id]    BIGINT          NOT NULL,
    [dv_inserted_date_time]  DATETIME        NOT NULL,
    [dv_insert_user]         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_hash]                CHAR (32)       NOT NULL,
    [dv_deleted]             BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

