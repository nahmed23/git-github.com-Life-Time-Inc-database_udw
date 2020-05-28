CREATE TABLE [dbo].[s_boss_mbr_contacts] (
    [s_boss_mbr_contacts_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)     NOT NULL,
    [mbr_contacts_id]        INT           NULL,
    [cust_code]              VARCHAR (10)  NULL,
    [mbr_code]               VARCHAR (10)  NULL,
    [first_name]             VARCHAR (50)  NULL,
    [last_name]              VARCHAR (50)  NULL,
    [dob]                    DATETIME      NULL,
    [relationship]           VARCHAR (40)  NULL,
    [type]                   VARCHAR (20)  NULL,
    [contactable_type]       VARCHAR (60)  NULL,
    [created_at]             DATETIME      NULL,
    [updated_at]             DATETIME      NULL,
    [email]                  VARCHAR (100) NULL,
    [dv_load_date_time]      DATETIME      NOT NULL,
    [dv_r_load_source_id]    BIGINT        NOT NULL,
    [dv_inserted_date_time]  DATETIME      NOT NULL,
    [dv_insert_user]         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]   DATETIME      NULL,
    [dv_update_user]         VARCHAR (50)  NULL,
    [dv_hash]                CHAR (32)     NOT NULL,
    [dv_deleted]             BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_mbr_contacts]([dv_batch_id] ASC);

