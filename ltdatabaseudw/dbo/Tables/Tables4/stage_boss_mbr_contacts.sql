CREATE TABLE [dbo].[stage_boss_mbr_contacts] (
    [stage_boss_mbr_contacts_id] BIGINT        NOT NULL,
    [id]                         INT           NULL,
    [cust_code]                  VARCHAR (10)  NULL,
    [mbr_code]                   VARCHAR (10)  NULL,
    [first_name]                 VARCHAR (50)  NULL,
    [last_name]                  VARCHAR (50)  NULL,
    [dob]                        DATETIME      NULL,
    [relationship]               VARCHAR (40)  NULL,
    [type]                       VARCHAR (20)  NULL,
    [contactable_id]             INT           NULL,
    [contactable_type]           VARCHAR (60)  NULL,
    [created_at]                 DATETIME      NULL,
    [updated_at]                 DATETIME      NULL,
    [search_id]                  VARCHAR (25)  NULL,
    [email]                      VARCHAR (100) NULL,
    [user_id]                    INT           NULL,
    [dv_batch_id]                BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

