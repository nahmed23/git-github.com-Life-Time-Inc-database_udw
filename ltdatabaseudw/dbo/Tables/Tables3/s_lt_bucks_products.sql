CREATE TABLE [dbo].[s_lt_bucks_products] (
    [s_lt_bucks_products_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)       NOT NULL,
    [product_id]              INT             NULL,
    [sku]                     NVARCHAR (20)   NULL,
    [name]                    NVARCHAR (100)  NULL,
    [product_desc]            NVARCHAR (4000) NULL,
    [price]                   DECIMAL (26, 6) NULL,
    [has_colors]              BIT             NULL,
    [has_sizes]               BIT             NULL,
    [product_order]           INT             NULL,
    [vendor]                  INT             NULL,
    [vendor_desc]             NVARCHAR (1000) NULL,
    [vendor_cost]             DECIMAL (26, 6) NULL,
    [vendor_drop_ship]        DECIMAL (26, 6) NULL,
    [vendor_est_frt]          DECIMAL (26, 6) NULL,
    [vendor_act_frt]          DECIMAL (26, 6) NULL,
    [msrp]                    DECIMAL (26, 6) NULL,
    [weight]                  DECIMAL (26, 6) NULL,
    [date_created]            SMALLDATETIME   NULL,
    [date_updated]            SMALLDATETIME   NULL,
    [active]                  BIT             NULL,
    [pgroup]                  INT             NULL,
    [must_obey_inventory]     BIT             NULL,
    [discontinued]            BIT             NULL,
    [on_closeout]             BIT             NULL,
    [asi_customer]            INT             NULL,
    [from_asi]                BIT             NULL,
    [country]                 INT             NULL,
    [image_filename]          NVARCHAR (50)   NULL,
    [per]                     VARCHAR (20)    NULL,
    [schart]                  INT             NULL,
    [is_flat]                 BIT             NULL,
    [promotion]               INT             NULL,
    [track_inventory]         BIT             NULL,
    [shipping_point_amount]   INT             NULL,
    [is_deleted]              BIT             NULL,
    [fulfillment_eligible]    BIT             NULL,
    [kit_eligible]            BIT             NULL,
    [last_modified_timestamp] DATETIME        NULL,
    [dv_load_date_time]       DATETIME        NOT NULL,
    [dv_batch_id]             BIGINT          NOT NULL,
    [dv_r_load_source_id]     BIGINT          NOT NULL,
    [dv_inserted_date_time]   DATETIME        NOT NULL,
    [dv_insert_user]          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]    DATETIME        NULL,
    [dv_update_user]          VARCHAR (50)    NULL,
    [dv_hash]                 CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_lt_bucks_products]
    ON [dbo].[s_lt_bucks_products]([bk_hash] ASC, [s_lt_bucks_products_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_lt_bucks_products]([dv_batch_id] ASC);

