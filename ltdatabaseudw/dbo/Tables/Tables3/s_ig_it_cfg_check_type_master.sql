CREATE TABLE [dbo].[s_ig_it_cfg_check_type_master] (
    [s_ig_it_cfg_check_type_master_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)     NOT NULL,
    [ent_id]                           INT           NULL,
    [check_type_id]                    INT           NULL,
    [check_type_name]                  NVARCHAR (16) NULL,
    [check_type_abbr_1]                NVARCHAR (7)  NULL,
    [check_type_abbr_2]                NVARCHAR (7)  NULL,
    [sales_tippable_flag]              BIT           NULL,
    [round_basis]                      INT           NULL,
    [round_type_id]                    SMALLINT      NULL,
    [row_version]                      BINARY (8)    NULL,
    [discount_id]                      INT           NULL,
    [dummy_modified_date_time]         DATETIME      NULL,
    [dv_load_date_time]                DATETIME      NOT NULL,
    [dv_r_load_source_id]              BIGINT        NOT NULL,
    [dv_inserted_date_time]            DATETIME      NOT NULL,
    [dv_insert_user]                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]             DATETIME      NULL,
    [dv_update_user]                   VARCHAR (50)  NULL,
    [dv_hash]                          CHAR (32)     NOT NULL,
    [dv_deleted]                       BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

