CREATE TABLE [dbo].[s_magento_sales_shipment_item] (
    [s_magento_sales_shipment_item_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [entity_id]                        INT             NULL,
    [row_total]                        DECIMAL (12, 4) NULL,
    [price]                            DECIMAL (12, 4) NULL,
    [weight]                           DECIMAL (12, 4) NULL,
    [qty]                              DECIMAL (12, 4) NULL,
    [additional_data]                  VARCHAR (8000)  NULL,
    [description]                      VARCHAR (8000)  NULL,
    [name]                             VARCHAR (255)   NULL,
    [dummy_modified_date_time]         DATETIME        NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_r_load_source_id]              BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_hash]                          CHAR (32)       NOT NULL,
    [dv_deleted]                       BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

