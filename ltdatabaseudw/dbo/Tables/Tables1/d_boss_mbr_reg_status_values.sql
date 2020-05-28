CREATE TABLE [dbo].[d_boss_mbr_reg_status_values] (
    [d_boss_mbr_reg_status_values_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [mbr_reg_status_values_id]        INT           NULL,
    [created_dim_date_key]            CHAR (8)      NULL,
    [created_dim_time_key]            CHAR (8)      NULL,
    [d_mbr_reg_status_types_bk_hash]  CHAR (32)     NULL,
    [end_dim_date_key]                CHAR (8)      NULL,
    [end_dim_time_key]                CHAR (8)      NULL,
    [mbr_reg_status_values_cust_code] VARCHAR (10)  NULL,
    [mbr_reg_status_values_mbr_code]  VARCHAR (10)  NULL,
    [mbr_reg_status_values_value]     VARCHAR (100) NULL,
    [reg_status_type_id]              INT           NULL,
    [start_dim_date_key]              CHAR (8)      NULL,
    [start_dim_time_key]              CHAR (8)      NULL,
    [updated_dim_date_key]            CHAR (8)      NULL,
    [updated_dim_time_key]            CHAR (8)      NULL,
    [p_boss_mbr_reg_status_values_id] BIGINT        NOT NULL,
    [deleted_flag]                    INT           NULL,
    [dv_load_date_time]               DATETIME      NULL,
    [dv_load_end_date_time]           DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_mbr_reg_status_values]([dv_batch_id] ASC);

