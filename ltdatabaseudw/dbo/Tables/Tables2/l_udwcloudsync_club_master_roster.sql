CREATE TABLE [dbo].[l_udwcloudsync_club_master_roster] (
    [l_udwcloudsync_club_master_roster_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [client_id]                                 NVARCHAR (4000) NULL,
    [it_code]                                   NVARCHAR (4000) NULL,
    [mms_club_id]                               INT             NULL,
    [old_kronos_id]                             NVARCHAR (4000) NULL,
    [sales_code]                                NVARCHAR (4000) NULL,
    [val_activity_area_id_lookup]               NVARCHAR (4000) NULL,
    [val_checkin_group_id_lookup]               NVARCHAR (4000) NULL,
    [val_club_type_id_lookup]                   NVARCHAR (4000) NULL,
    [val_country_id_lookup]                     NVARCHAR (4000) NULL,
    [val_currency_code_id_lookup]               NVARCHAR (4000) NULL,
    [val_currency_code_id_lookup_currency_code] NVARCHAR (4000) NULL,
    [val_cw_region_id_lookup]                   NVARCHAR (4000) NULL,
    [val_enrollment_type_id_lookup]             NVARCHAR (4000) NULL,
    [val_member_activity_region_id_lookup]      NVARCHAR (4000) NULL,
    [val_pre_sale_id_lookup]                    NVARCHAR (4000) NULL,
    [val_ptrcl_area_id_lookup]                  NVARCHAR (4000) NULL,
    [val_region_id_lookup]                      NVARCHAR (4000) NULL,
    [val_sales_area_id_lookup]                  NVARCHAR (4000) NULL,
    [val_state_id_lookup]                       NVARCHAR (4000) NULL,
    [val_time_zone_id_lookup]                   NVARCHAR (4000) NULL,
    [workday_region_id]                         NVARCHAR (4000) NULL,
    [workflow_instance_id]                      NVARCHAR (4000) NULL,
    [spabiz_store_num]                          INT             NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_r_load_source_id]                       BIGINT          NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL,
    [dv_hash]                                   CHAR (32)       NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_udwcloudsync_club_master_roster]
    ON [dbo].[l_udwcloudsync_club_master_roster]([bk_hash] ASC, [l_udwcloudsync_club_master_roster_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_udwcloudsync_club_master_roster]([dv_batch_id] ASC);

