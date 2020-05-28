﻿CREATE TABLE [dbo].[stage_hash_exerp_product_product_group] (
    [stage_hash_exerp_product_product_group_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)      NOT NULL,
    [product_id]                                VARCHAR (4000) NULL,
    [product_group_id]                          INT            NULL,
    [ets]                                       BIGINT         NULL,
    [dummy_modified_date_time]                  DATETIME       NULL,
    [dv_load_date_time]                         DATETIME       NOT NULL,
    [dv_updated_date_time]                      DATETIME       NULL,
    [dv_update_user]                            VARCHAR (50)   NULL,
    [dv_batch_id]                               BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

