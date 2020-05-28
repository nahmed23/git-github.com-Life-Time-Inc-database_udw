CREATE TABLE [dbo].[l_ig_it_cfg_tender_master] (
    [l_ig_it_cfg_tender_master_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [ent_id]                       INT          NULL,
    [tender_id]                    INT          NULL,
    [verification_code_id]         INT          NULL,
    [franking_code_id]             INT          NULL,
    [security_id]                  INT          NULL,
    [over_tender_code_id]          SMALLINT     NULL,
    [open_cashdrwr_code_id]        SMALLINT     NULL,
    [check_type_id]                INT          NULL,
    [price_level_id]               INT          NULL,
    [discoup_id]                   INT          NULL,
    [post_site_id]                 INT          NULL,
    [tender_class_id]              INT          NULL,
    [store_id]                     INT          NULL,
    [additional_check_id_code_id]  TINYINT      NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_cfg_tender_master]([dv_batch_id] ASC);

