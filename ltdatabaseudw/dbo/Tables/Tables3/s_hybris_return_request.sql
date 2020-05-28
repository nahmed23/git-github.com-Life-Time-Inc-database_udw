﻿CREATE TABLE [dbo].[s_hybris_return_request] (
    [s_hybris_return_request_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [hjmpts]                     BIGINT         NULL,
    [created_ts]                 DATETIME       NULL,
    [modified_ts]                DATETIME       NULL,
    [return_request_pk]          BIGINT         NULL,
    [p_code]                     NVARCHAR (255) NULL,
    [p_rma]                      NVARCHAR (255) NULL,
    [p_replacement_order]        BIGINT         NULL,
    [p_currency]                 BIGINT         NULL,
    [p_status]                   BIGINT         NULL,
    [p_return_label]             BIGINT         NULL,
    [p_return_warehouse]         BIGINT         NULL,
    [p_order_pos]                INT            NULL,
    [p_order]                    BIGINT         NULL,
    [p_refund_delivery_cost]     TINYINT        NULL,
    [acl_ts]                     BIGINT         NULL,
    [prop_ts]                    BIGINT         NULL,
    [dv_load_date_time]          DATETIME       NOT NULL,
    [dv_r_load_source_id]        BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL,
    [dv_hash]                    CHAR (32)      NOT NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_return_request]
    ON [dbo].[s_hybris_return_request]([bk_hash] ASC, [s_hybris_return_request_id] ASC);

