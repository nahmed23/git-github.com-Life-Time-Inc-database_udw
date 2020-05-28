CREATE TABLE [dbo].[stage_hash_boss_mbr_contacts] (
    [stage_hash_boss_mbr_contacts_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [id]                              INT           NULL,
    [cust_code]                       VARCHAR (10)  NULL,
    [mbr_code]                        VARCHAR (10)  NULL,
    [first_name]                      VARCHAR (50)  NULL,
    [last_name]                       VARCHAR (50)  NULL,
    [dob]                             DATETIME      NULL,
    [relationship]                    VARCHAR (40)  NULL,
    [type]                            VARCHAR (20)  NULL,
    [contactable_id]                  INT           NULL,
    [contactable_type]                VARCHAR (60)  NULL,
    [created_at]                      DATETIME      NULL,
    [updated_at]                      DATETIME      NULL,
    [search_id]                       VARCHAR (25)  NULL,
    [email]                           VARCHAR (100) NULL,
    [user_id]                         INT           NULL,
    [dv_load_date_time]               DATETIME      NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

