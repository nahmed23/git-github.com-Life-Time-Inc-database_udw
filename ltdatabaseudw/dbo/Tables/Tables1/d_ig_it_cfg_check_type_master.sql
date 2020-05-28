CREATE TABLE [dbo].[d_ig_it_cfg_check_type_master] (
    [d_ig_it_cfg_check_type_master_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)     NOT NULL,
    [dim_ig_it_cfg_check_type_master_key] VARCHAR (32)  NULL,
    [ent_id]                              INT           NULL,
    [check_type_id]                       INT           NULL,
    [check_type_abbr_1]                   NVARCHAR (7)  NULL,
    [check_type_abbr_2]                   NVARCHAR (7)  NULL,
    [check_type_name]                     NVARCHAR (16) NULL,
    [default_price_level_id]              INT           NULL,
    [default_secondary_id]                INT           NULL,
    [discount_id]                         INT           NULL,
    [round_basis]                         INT           NULL,
    [round_type_id]                       SMALLINT      NULL,
    [row_version]                         BINARY (8)    NULL,
    [sales_tippable_flag]                 CHAR (1)      NULL,
    [store_id]                            INT           NULL,
    [p_ig_it_cfg_check_type_master_id]    BIGINT        NOT NULL,
    [deleted_flag]                        INT           NULL,
    [dv_load_date_time]                   DATETIME      NULL,
    [dv_load_end_date_time]               DATETIME      NULL,
    [dv_batch_id]                         BIGINT        NOT NULL,
    [dv_inserted_date_time]               DATETIME      NOT NULL,
    [dv_insert_user]                      VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                DATETIME      NULL,
    [dv_update_user]                      VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

