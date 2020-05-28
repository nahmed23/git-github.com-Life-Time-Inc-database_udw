﻿CREATE TABLE [dbo].[l_exerp_subscription] (
    [l_exerp_subscription_id]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [person_id]                 VARCHAR (4000) NULL,
    [subscription_id]           VARCHAR (4000) NULL,
    [center_id]                 INT            NULL,
    [product_id]                VARCHAR (4000) NULL,
    [freeze_period_product_id]  VARCHAR (4000) NULL,
    [transfer_subscription_id]  VARCHAR (4000) NULL,
    [extension_subscription_id] VARCHAR (4000) NULL,
    [reassign_subscription_id]  VARCHAR (4000) NULL,
    [stop_person_id]            VARCHAR (4000) NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_deleted]                BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));
