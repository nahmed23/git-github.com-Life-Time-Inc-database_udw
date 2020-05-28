CREATE TABLE [dbo].[d_spabiz_ap_group_data] (
    [d_spabiz_ap_group_data_id]                                BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                  CHAR (32)    NOT NULL,
    [dim_spabiz_appointment_group_bridge_dim_spabiz_staff_key] CHAR (32)    NULL,
    [ap_group_data_id]                                         BIGINT       NULL,
    [store_number]                                             BIGINT       NULL,
    [dim_spabiz_appointment_group_key]                         CHAR (32)    NULL,
    [dim_spabiz_staff_key]                                     CHAR (32)    NULL,
    [dim_spabiz_store_key]                                     CHAR (32)    NULL,
    [edit_date_time]                                           DATETIME     NULL,
    [l_spabiz_ap_group_data_ap_group_id]                       BIGINT       NULL,
    [l_spabiz_ap_group_data_staff_id]                          BIGINT       NULL,
    [p_spabiz_ap_group_data_id]                                BIGINT       NOT NULL,
    [dv_load_date_time]                                        DATETIME     NULL,
    [dv_load_end_date_time]                                    DATETIME     NULL,
    [dv_batch_id]                                              BIGINT       NOT NULL,
    [dv_inserted_date_time]                                    DATETIME     NOT NULL,
    [dv_insert_user]                                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                                     DATETIME     NULL,
    [dv_update_user]                                           VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ap_group_data]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_ap_group_data]([dv_batch_id]);

