CREATE TABLE [dbo].[stage_hash_mms_ChildCenterUsageActivityArea] (
    [stage_hash_mms_ChildCenterUsageActivityArea_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)    NOT NULL,
    [ChildCenterUsageActivityAreaID]                 INT          NULL,
    [ChildCenterUsageID]                             INT          NULL,
    [ValActivityAreaID]                              SMALLINT     NULL,
    [InsertedDatetime]                               DATETIME     NULL,
    [UpdatedDateTime]                                DATETIME     NULL,
    [dv_load_date_time]                              DATETIME     NOT NULL,
    [dv_updated_date_time]                           DATETIME     NULL,
    [dv_update_user]                                 VARCHAR (50) NULL,
    [dv_batch_id]                                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

