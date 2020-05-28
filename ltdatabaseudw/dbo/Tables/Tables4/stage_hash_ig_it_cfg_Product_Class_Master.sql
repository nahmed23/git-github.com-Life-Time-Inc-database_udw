CREATE TABLE [dbo].[stage_hash_ig_it_cfg_Product_Class_Master] (
    [stage_hash_ig_it_cfg_Product_Class_Master_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [ent_id]                                       INT           NULL,
    [prod_class_id]                                INT           NULL,
    [prod_class_name]                              NVARCHAR (16) NULL,
    [default_rev_cat_id]                           INT           NULL,
    [default_tax_grp_id]                           INT           NULL,
    [default_sec_id]                               INT           NULL,
    [default_menu_item_group_id]                   INT           NULL,
    [default_rpt_cat_id]                           INT           NULL,
    [store_id]                                     INT           NULL,
    [item_restricted_flag]                         BIT           NULL,
    [row_version]                                  BINARY (8)    NULL,
    [track_id]                                     BIGINT        NULL,
    [inserted_date_time]                           DATETIME      NULL,
    [updated_date_time]                            DATETIME      NULL,
    [track_action]                                 NVARCHAR (1)  NULL,
    [dv_load_date_time]                            DATETIME      NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

