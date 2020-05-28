CREATE TABLE [dbo].[stage_hash_ig_it_cfg_Profit_Center_Group_Join] (
    [stage_hash_ig_it_cfg_Profit_Center_Group_Join_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)    NOT NULL,
    [ent_id]                                           INT          NULL,
    [profit_ctr_grp_id]                                INT          NULL,
    [profit_center_id]                                 INT          NULL,
    [jan_one]                                          DATETIME     NULL,
    [dv_load_date_time]                                DATETIME     NOT NULL,
    [dv_inserted_date_time]                            DATETIME     NOT NULL,
    [dv_insert_user]                                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                             DATETIME     NULL,
    [dv_update_user]                                   VARCHAR (50) NULL,
    [dv_batch_id]                                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

