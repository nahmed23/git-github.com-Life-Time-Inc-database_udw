﻿CREATE TABLE [dbo].[stage_hash_lt_bucks_Products] (
    [stage_hash_lt_bucks_Products_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [product_id]                      INT             NULL,
    [product_sku]                     NVARCHAR (20)   NULL,
    [product_name]                    NVARCHAR (100)  NULL,
    [product_desc]                    NVARCHAR (4000) NULL,
    [product_price]                   DECIMAL (26, 6) NULL,
    [product_has_colors]              BIT             NULL,
    [product_has_sizes]               BIT             NULL,
    [product_order]                   INT             NULL,
    [product_vendor]                  INT             NULL,
    [product_vendor_id]               NVARCHAR (20)   NULL,
    [product_vendor_desc]             NVARCHAR (1000) NULL,
    [product_vendor_cost]             DECIMAL (26, 6) NULL,
    [product_vendor_drop_ship]        DECIMAL (26, 6) NULL,
    [product_vendor_est_frt]          DECIMAL (26, 6) NULL,
    [product_vendor_act_frt]          DECIMAL (26, 6) NULL,
    [product_msrp]                    DECIMAL (26, 6) NULL,
    [product_weight]                  DECIMAL (26, 6) NULL,
    [product_date_created]            SMALLDATETIME   NULL,
    [product_date_updated]            SMALLDATETIME   NULL,
    [product_active]                  BIT             NULL,
    [product_pgroup]                  INT             NULL,
    [product_must_obey_inventory]     BIT             NULL,
    [product_discontinued]            BIT             NULL,
    [product_on_closeout]             BIT             NULL,
    [product_asi_customer]            INT             NULL,
    [product_from_asi]                BIT             NULL,
    [product_country]                 INT             NULL,
    [product_image_filename]          NVARCHAR (50)   NULL,
    [product_per]                     VARCHAR (20)    NULL,
    [product_schart]                  INT             NULL,
    [product_isFlat]                  BIT             NULL,
    [product_promotion]               INT             NULL,
    [product_track_inventory]         BIT             NULL,
    [product_shipping_point_amount]   INT             NULL,
    [product_last_user]               INT             NULL,
    [product_isDeleted]               BIT             NULL,
    [product_fulfillment_eligible]    BIT             NULL,
    [product_kit_eligible]            BIT             NULL,
    [LastModifiedTimestamp]           DATETIME        NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));
