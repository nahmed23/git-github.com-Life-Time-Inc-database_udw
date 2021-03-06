﻿CREATE TABLE [dbo].[d_exerp_relation_state_log] (
    [d_exerp_relation_state_log_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [relation_state_log_id]         INT            NULL,
    [center_id]                     INT            NULL,
    [company_id]                    VARCHAR (4000) NULL,
    [d_exerp_relation_bk_hash]      VARCHAR (32)   NULL,
    [dim_club_key]                  VARCHAR (32)   NULL,
    [dim_mms_company_key]           VARCHAR (32)   NULL,
    [dim_mms_member_key]            VARCHAR (32)   NULL,
    [ets]                           BIGINT         NULL,
    [from_datetime]                 DATETIME       NULL,
    [from_dim_date_key]             VARCHAR (8)    NULL,
    [from_dim_time_key]             INT            NULL,
    [person_id]                     VARCHAR (4000) NULL,
    [relation_id]                   VARCHAR (4000) NULL,
    [relation_state_log_status]     VARCHAR (4000) NULL,
    [relation_state_log_type]       VARCHAR (4000) NULL,
    [relative_dim_mms_member_key]   VARCHAR (32)   NULL,
    [relative_id]                   VARCHAR (4000) NULL,
    [relative_type]                 VARCHAR (4000) NULL,
    [p_exerp_relation_state_log_id] BIGINT         NOT NULL,
    [deleted_flag]                  INT            NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

