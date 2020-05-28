﻿CREATE TABLE [dbo].[dim_nmo_hub_task_status] (
    [dim_nmo_hub_task_status_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [activation_dim_date_key]         VARCHAR (8)    NULL,
    [activation_dim_time_key]         INT            NULL,
    [created_dim_date_key]            VARCHAR (8)    NULL,
    [created_dim_time_key]            INT            NULL,
    [creator_party_id]                INT            NULL,
    [department_title]                NVARCHAR (255) NULL,
    [dim_club_key]                    VARCHAR (32)   NULL,
    [dim_employee_key]                VARCHAR (32)   NULL,
    [dim_mms_member_key]              VARCHAR (32)   NULL,
    [dim_nmo_hub_task_department_key] VARCHAR (32)   NULL,
    [dim_nmo_hub_task_key]            VARCHAR (32)   NULL,
    [dim_nmo_hub_task_status_key]     VARCHAR (32)   NULL,
    [dim_nmo_hub_task_type_key]       VARCHAR (32)   NULL,
    [due_dim_date_key]                VARCHAR (8)    NULL,
    [due_dim_time_key]                INT            NULL,
    [expiration_dim_date_key]         VARCHAR (8)    NULL,
    [expiration_dim_time_key]         INT            NULL,
    [party_id]                        INT            NULL,
    [resolution_dim_date_key]         VARCHAR (8)    NULL,
    [resolution_dim_time_key]         INT            NULL,
    [status]                          NVARCHAR (200) NULL,
    [dv_load_date_time]               DATETIME       NULL,
    [dv_load_end_date_time]           DATETIME       NULL,
    [dv_batch_id]                     BIGINT         NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_nmo_hub_task_status_key]));
