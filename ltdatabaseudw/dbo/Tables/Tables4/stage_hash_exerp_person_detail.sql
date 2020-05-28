CREATE TABLE [dbo].[stage_hash_exerp_person_detail] (
    [stage_hash_exerp_person_detail_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [person_id]                         VARCHAR (4000) NULL,
    [address1]                          VARCHAR (4000) NULL,
    [address2]                          VARCHAR (4000) NULL,
    [address3]                          VARCHAR (4000) NULL,
    [work_phone]                        VARCHAR (4000) NULL,
    [mobile_phone]                      VARCHAR (4000) NULL,
    [home_phone]                        VARCHAR (4000) NULL,
    [email]                             VARCHAR (4000) NULL,
    [full_name]                         VARCHAR (4000) NULL,
    [firstname]                         VARCHAR (4000) NULL,
    [lastname]                          VARCHAR (4000) NULL,
    [center_id]                         INT            NULL,
    [ets]                               BIGINT         NULL,
    [dummy_modified_date_time]          DATETIME       NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

