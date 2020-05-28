CREATE TABLE [dbo].[dim_membership_star_rank] (
    [dim_membership_star_rank_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [customer_name]               VARCHAR (101) NULL,
    [dim_mms_member_key]          VARCHAR (32)  NULL,
    [first_name]                  VARCHAR (50)  NULL,
    [gender]                      VARCHAR (1)   NULL,
    [join_date]                   DATETIME      NULL,
    [last_name]                   VARCHAR (50)  NULL,
    [member_id]                   INT           NULL,
    [membership_id]               INT           NULL,
    [val_star_rank_id]            INT           NULL,
    [dv_load_date_time]           DATETIME      NULL,
    [dv_load_end_date_time]       DATETIME      NULL,
    [dv_batch_id]                 BIGINT        NOT NULL,
    [dv_inserted_date_time]       DATETIME      NOT NULL,
    [dv_insert_user]              VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]        DATETIME      NULL,
    [dv_update_user]              VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

