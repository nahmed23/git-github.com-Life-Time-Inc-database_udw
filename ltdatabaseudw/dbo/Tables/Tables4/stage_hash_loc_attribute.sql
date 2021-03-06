﻿CREATE TABLE [dbo].[stage_hash_loc_attribute] (
    [stage_hash_loc_attribute_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)     NOT NULL,
    [attribute_id]                   BIGINT        NULL,
    [val_attribute_type_id]          BIGINT        NULL,
    [attribute_value]                VARCHAR (100) NULL,
    [udw_dim_location_attribute_key] VARCHAR (32)  NULL,
    [udw_business_key]               VARCHAR (32)  NULL,
    [udw_source_name]                VARCHAR (100) NULL,
    [created_date_time]              DATETIME      NULL,
    [created_by]                     VARCHAR (100) NULL,
    [last_updated_date_time]         DATETIME      NULL,
    [last_updated_by]                VARCHAR (100) NULL,
    [deleted_date_time]              DATETIME      NULL,
    [deleted_by]                     VARCHAR (100) NULL,
    [location_id]                    BIGINT        NULL,
    [managed_by_udw]                 BIT           NULL,
    [dv_load_date_time]              DATETIME      NOT NULL,
    [dv_batch_id]                    BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

