CREATE TABLE [dbo].[d_lt_bucks_users] (
    [d_lt_bucks_users_id]             BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [dim_lt_bucks_users_key]          CHAR (32)     NULL,
    [user_id]                         INT           NULL,
    [active_flag]                     VARCHAR (1)   NULL,
    [address_city]                    NVARCHAR (50) NULL,
    [address_line_1]                  NVARCHAR (75) NULL,
    [address_line_2]                  NVARCHAR (75) NULL,
    [address_postal_code]             NVARCHAR (15) NULL,
    [address_state]                   NVARCHAR (50) NULL,
    [current_points]                  INT           NULL,
    [dim_mms_member_key]              CHAR (32)     NULL,
    [email]                           NVARCHAR (50) NULL,
    [first_name]                      NVARCHAR (50) NULL,
    [last_name]                       NVARCHAR (50) NULL,
    [referring_dim_lt_bucks_user_key] CHAR (32)     NULL,
    [register_date_time]              DATETIME      NULL,
    [user_parent]                     INT           NULL,
    [user_phone]                      NVARCHAR (50) NULL,
    [user_type]                       VARCHAR (30)  NULL,
    [user_username]                   NVARCHAR (50) NULL,
    [p_lt_bucks_users_id]             BIGINT        NOT NULL,
    [dv_load_date_time]               DATETIME      NULL,
    [dv_load_end_date_time]           DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_lt_bucks_users]([dv_batch_id] ASC);

