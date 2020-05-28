CREATE TABLE [dbo].[l_spabiz_product] (
    [l_spabiz_product_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [product_id]            DECIMAL (26, 6) NULL,
    [store_id]              DECIMAL (26, 6) NULL,
    [vendor_id]             DECIMAL (26, 6) NULL,
    [man_id]                DECIMAL (26, 6) NULL,
    [commission_id]         DECIMAL (26, 6) NULL,
    [dept_cat]              DECIMAL (26, 6) NULL,
    [search_cat]            DECIMAL (26, 6) NULL,
    [min_max_id]            DECIMAL (26, 6) NULL,
    [default_staff_id]      DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [gl_account]            VARCHAR (45)    NULL,
    [enter_id]              DECIMAL (26, 6) NULL,
    [link_id]               DECIMAL (26, 6) NULL,
    [parent_id]             DECIMAL (26, 6) NULL,
    [product_backup_id]     DECIMAL (26, 6) NULL,
    [backup_prod_id]        DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_l_spabiz_product]
    ON [dbo].[l_spabiz_product]([bk_hash] ASC, [l_spabiz_product_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_product]([dv_batch_id] ASC);

