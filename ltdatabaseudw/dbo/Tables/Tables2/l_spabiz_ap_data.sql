CREATE TABLE [dbo].[l_spabiz_ap_data] (
    [l_spabiz_ap_data_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ap_data_id]            DECIMAL (26, 6) NULL,
    [store_id]              DECIMAL (26, 6) NULL,
    [ap_id]                 DECIMAL (26, 6) NULL,
    [service_id]            DECIMAL (26, 6) NULL,
    [cust_id]               DECIMAL (26, 6) NULL,
    [staff_id]              DECIMAL (26, 6) NULL,
    [parent_id]             DECIMAL (26, 6) NULL,
    [staff_added]           DECIMAL (26, 6) NULL,
    [retention]             DECIMAL (26, 6) NULL,
    [ticket_data_id]        DECIMAL (26, 6) NULL,
    [resource_id]           DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [by_staff_id]           DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_spabiz_ap_data]
    ON [dbo].[l_spabiz_ap_data]([bk_hash] ASC, [l_spabiz_ap_data_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_ap_data]([dv_batch_id] ASC);

