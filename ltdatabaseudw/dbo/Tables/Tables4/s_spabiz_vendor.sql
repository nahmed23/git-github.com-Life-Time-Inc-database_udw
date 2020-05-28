CREATE TABLE [dbo].[s_spabiz_vendor] (
    [s_spabiz_vendor_id]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [vendor_id]             DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [vendor_delete]         DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (150)   NULL,
    [quick_id]              VARCHAR (150)   NULL,
    [phone]                 VARCHAR (150)   NULL,
    [fax]                   VARCHAR (150)   NULL,
    [contact_1]             VARCHAR (150)   NULL,
    [contact_1_title]       VARCHAR (90)    NULL,
    [contact_1_tel]         VARCHAR (45)    NULL,
    [contact_1_ext]         VARCHAR (12)    NULL,
    [contact_2]             VARCHAR (150)   NULL,
    [contact_2_title]       VARCHAR (90)    NULL,
    [contact_2_tel]         VARCHAR (45)    NULL,
    [contact_2_ext]         VARCHAR (12)    NULL,
    [order_freq]            DECIMAL (26, 6) NULL,
    [po_method]             DECIMAL (26, 6) NULL,
    [address_1]             VARCHAR (765)   NULL,
    [address_2]             VARCHAR (150)   NULL,
    [city]                  VARCHAR (150)   NULL,
    [st]                    VARCHAR (150)   NULL,
    [zip]                   VARCHAR (150)   NULL,
    [email]                 VARCHAR (150)   NULL,
    [customer_num]          VARCHAR (150)   NULL,
    [day_1]                 DECIMAL (26, 6) NULL,
    [day_2]                 DECIMAL (26, 6) NULL,
    [day_3]                 DECIMAL (26, 6) NULL,
    [day_4]                 DECIMAL (26, 6) NULL,
    [day_5]                 DECIMAL (26, 6) NULL,
    [day_6]                 DECIMAL (26, 6) NULL,
    [day_7]                 DECIMAL (26, 6) NULL,
    [week_end]              DECIMAL (26, 6) NULL,
    [month_end]             DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [new_id]                DECIMAL (26, 6) NULL,
    [vendor_backup_id]      DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_vendor]
    ON [dbo].[s_spabiz_vendor]([bk_hash] ASC, [s_spabiz_vendor_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_vendor]([dv_batch_id] ASC);

