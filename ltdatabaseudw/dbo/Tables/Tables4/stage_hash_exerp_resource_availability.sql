﻿CREATE TABLE [dbo].[stage_hash_exerp_resource_availability] (
    [stage_hash_exerp_resource_availability_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)      NOT NULL,
    [resource_id]                               VARCHAR (4000) NULL,
    [resource_group_id]                         INT            NULL,
    [availability_type]                         VARCHAR (4000) NULL,
    [value]                                     VARCHAR (4000) NULL,
    [from_time]                                 VARCHAR (4000) NULL,
    [to_time]                                   VARCHAR (4000) NULL,
    [dummy_modified_date_time]                  DATETIME       NULL,
    [dv_load_date_time]                         DATETIME       NOT NULL,
    [dv_inserted_date_time]                     DATETIME       NOT NULL,
    [dv_insert_user]                            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                      DATETIME       NULL,
    [dv_update_user]                            VARCHAR (50)   NULL,
    [dv_batch_id]                               BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

