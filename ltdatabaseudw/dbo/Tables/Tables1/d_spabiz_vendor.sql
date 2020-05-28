CREATE TABLE [dbo].[d_spabiz_vendor] (
    [d_spabiz_vendor_id]                BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)     NOT NULL,
    [dim_spabiz_vendor_key]             CHAR (32)     NULL,
    [vendor_id]                         BIGINT        NULL,
    [store_number]                      BIGINT        NULL,
    [address_1]                         VARCHAR (765) NULL,
    [address_2]                         VARCHAR (150) NULL,
    [city]                              VARCHAR (150) NULL,
    [customer_number]                   VARCHAR (150) NULL,
    [deleted_date_time]                 DATETIME      NULL,
    [deleted_flag]                      CHAR (1)      NULL,
    [dim_spabiz_store_key]              CHAR (32)     NULL,
    [edit_date_time]                    DATETIME      NULL,
    [email]                             VARCHAR (150) NULL,
    [fax]                               VARCHAR (150) NULL,
    [name]                              VARCHAR (150) NULL,
    [phone_number]                      VARCHAR (150) NULL,
    [postal_code]                       VARCHAR (150) NULL,
    [primary_contact]                   VARCHAR (150) NULL,
    [primary_contact_phone_extension]   VARCHAR (12)  NULL,
    [primary_contact_phone_number]      VARCHAR (45)  NULL,
    [primary_contact_title]             VARCHAR (90)  NULL,
    [quick_id]                          VARCHAR (150) NULL,
    [secondary_contact]                 VARCHAR (150) NULL,
    [secondary_contact_phone_extension] VARCHAR (12)  NULL,
    [secondary_contact_phone_number]    VARCHAR (45)  NULL,
    [secondary_contact_title]           VARCHAR (90)  NULL,
    [state]                             VARCHAR (150) NULL,
    [p_spabiz_vendor_id]                BIGINT        NOT NULL,
    [dv_load_date_time]                 DATETIME      NULL,
    [dv_load_end_date_time]             DATETIME      NULL,
    [dv_batch_id]                       BIGINT        NOT NULL,
    [dv_inserted_date_time]             DATETIME      NOT NULL,
    [dv_insert_user]                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]              DATETIME      NULL,
    [dv_update_user]                    VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_vendor]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_vendor]([dv_batch_id]);

