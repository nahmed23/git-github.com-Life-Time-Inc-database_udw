CREATE TABLE [dbo].[d_boss_mbr_contacts] (
    [d_boss_mbr_contacts_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)     NOT NULL,
    [mbr_contacts_id]        INT           NULL,
    [contactable_id]         INT           NULL,
    [contactable_type]       VARCHAR (60)  NULL,
    [created_dim_date_key]   CHAR (8)      NULL,
    [created_dim_time_key]   CHAR (8)      NULL,
    [dob_dim_date_key]       CHAR (8)      NULL,
    [dob_dim_time_key]       CHAR (8)      NULL,
    [email]                  VARCHAR (100) NULL,
    [first_name]             VARCHAR (50)  NULL,
    [last_name]              VARCHAR (50)  NULL,
    [mbr_contacts_cust_code] VARCHAR (10)  NULL,
    [mbr_contacts_mbr_code]  VARCHAR (10)  NULL,
    [mbr_contacts_type]      VARCHAR (20)  NULL,
    [relationship]           VARCHAR (40)  NULL,
    [search_id]              VARCHAR (25)  NULL,
    [updated_dim_date_key]   CHAR (8)      NULL,
    [updated_dim_time_key]   CHAR (8)      NULL,
    [user_id]                INT           NULL,
    [p_boss_mbr_contacts_id] BIGINT        NOT NULL,
    [deleted_flag]           INT           NULL,
    [dv_load_date_time]      DATETIME      NULL,
    [dv_load_end_date_time]  DATETIME      NULL,
    [dv_batch_id]            BIGINT        NOT NULL,
    [dv_inserted_date_time]  DATETIME      NOT NULL,
    [dv_insert_user]         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]   DATETIME      NULL,
    [dv_update_user]         VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_mbr_contacts]([dv_batch_id] ASC);

