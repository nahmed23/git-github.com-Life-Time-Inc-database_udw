CREATE TABLE [dbo].[l_mms_club] (
    [l_mms_club_id]                 BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [club_id]                       INT          NULL,
    [val_region_id]                 INT          NULL,
    [statement_message_id]          INT          NULL,
    [val_club_type_id]              INT          NULL,
    [val_statement_type_id]         INT          NULL,
    [val_pre_sale_id]               INT          NULL,
    [val_time_zone_id]              INT          NULL,
    [val_cw_region_id]              INT          NULL,
    [eft_group_id]                  INT          NULL,
    [gl_tax_id]                     INT          NULL,
    [gl_club_id]                    INT          NULL,
    [site_id]                       INT          NULL,
    [val_member_activity_region_id] INT          NULL,
    [ig_store_id]                   INT          NULL,
    [val_sales_area_id]             INT          NULL,
    [val_pt_rcl_area_id]            INT          NULL,
    [val_currency_code_id]          INT          NULL,
    [ltf_resource_id]               INT          NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_r_load_source_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_hash]                       CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_club]
    ON [dbo].[l_mms_club]([bk_hash] ASC, [l_mms_club_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_club]([dv_batch_id] ASC);

