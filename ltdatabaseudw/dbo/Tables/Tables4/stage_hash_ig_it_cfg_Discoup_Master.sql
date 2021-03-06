﻿CREATE TABLE [dbo].[stage_hash_ig_it_cfg_Discoup_Master] (
    [stage_hash_ig_it_cfg_Discoup_Master_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [ent_id]                                 INT             NULL,
    [discoup_id]                             INT             NULL,
    [discoup_name]                           NVARCHAR (16)   NULL,
    [discoup_abbr1]                          NVARCHAR (7)    NULL,
    [discoup_abbr2]                          NVARCHAR (7)    NULL,
    [discoup_type_code_id]                   SMALLINT        NULL,
    [discoup_item_level_code_id]             SMALLINT        NULL,
    [discoup_open_code_id]                   SMALLINT        NULL,
    [discoup_pct_amt_code_id]                SMALLINT        NULL,
    [discoup_percent]                        DECIMAL (26, 6) NULL,
    [discoup_max_percent]                    DECIMAL (26, 6) NULL,
    [discoup_amt]                            DECIMAL (26, 6) NULL,
    [discoup_max_amt]                        DECIMAL (26, 6) NULL,
    [post_acct_no]                           NVARCHAR (30)   NULL,
    [prompt_extra_data_flag]                 CHAR (1)        NULL,
    [threshhold_amt]                         DECIMAL (26, 6) NULL,
    [profit_ctr_grp_id]                      INT             NULL,
    [round_basis]                            INT             NULL,
    [round_type_id]                          SMALLINT        NULL,
    [store_id]                               INT             NULL,
    [assoc_tender_id]                        INT             NULL,
    [food_rev_class_flag]                    CHAR (1)        NULL,
    [bev_rev_class_flag]                     CHAR (1)        NULL,
    [soda_rev_class_flag]                    CHAR (1)        NULL,
    [other_rev_class_flag]                   CHAR (1)        NULL,
    [exclusive_flag]                         CHAR (1)        NULL,
    [discount_extra_prompt_code]             TINYINT         NULL,
    [row_version]                            BINARY (8)      NULL,
    [security_id]                            INT             NULL,
    [jan_one]                                DATETIME        NULL,
    [dv_load_date_time]                      DATETIME        NOT NULL,
    [dv_inserted_date_time]                  DATETIME        NOT NULL,
    [dv_insert_user]                         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL,
    [dv_batch_id]                            BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

