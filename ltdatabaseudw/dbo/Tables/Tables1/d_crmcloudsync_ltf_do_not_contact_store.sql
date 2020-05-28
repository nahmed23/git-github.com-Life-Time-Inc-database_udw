﻿CREATE TABLE [dbo].[d_crmcloudsync_ltf_do_not_contact_store] (
    [d_crmcloudsync_ltf_do_not_contact_store_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)      NOT NULL,
    [dim_crm_ltf_do_not_contact_store_key]       VARCHAR (32)   NULL,
    [ltf_do_not_contact_store_id]                VARCHAR (36)   NULL,
    [created_dim_date_key]                       VARCHAR (8)    NULL,
    [created_dim_time_key]                       INT            NULL,
    [created_on]                                 DATETIME       NULL,
    [dim_crm_contact_key]                        VARCHAR (32)   NULL,
    [dim_crm_lead_key]                           VARCHAR (32)   NULL,
    [ltf_email_address1]                         NVARCHAR (100) NULL,
    [p_crmcloudsync_ltf_do_not_contact_store_id] BIGINT         NOT NULL,
    [deleted_flag]                               INT            NULL,
    [dv_load_date_time]                          DATETIME       NULL,
    [dv_load_end_date_time]                      DATETIME       NULL,
    [dv_batch_id]                                BIGINT         NOT NULL,
    [dv_inserted_date_time]                      DATETIME       NOT NULL,
    [dv_insert_user]                             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                       DATETIME       NULL,
    [dv_update_user]                             VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

