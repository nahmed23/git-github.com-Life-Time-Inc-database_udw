CREATE TABLE [dbo].[s_exerp_person_detail] (
    [s_exerp_person_detail_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [person_id]                VARCHAR (4000) NULL,
    [address_1]                VARCHAR (4000) NULL,
    [address_2]                VARCHAR (4000) NULL,
    [address_3]                VARCHAR (4000) NULL,
    [work_phone]               VARCHAR (4000) NULL,
    [mobile_phone]             VARCHAR (4000) NULL,
    [home_phone]               VARCHAR (4000) NULL,
    [email]                    VARCHAR (4000) NULL,
    [full_name]                VARCHAR (4000) NULL,
    [first_name]               VARCHAR (4000) NULL,
    [last_name]                VARCHAR (4000) NULL,
    [ets]                      BIGINT         NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_r_load_source_id]      BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_hash]                  CHAR (32)      NOT NULL,
    [dv_deleted]               BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

