CREATE TABLE [dbo].[stage_hash_mms_HRMMSClub] (
    [stage_hash_mms_HRMMSClub_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)     NOT NULL,
    [HRMMSClubID]                 INT           NULL,
    [HRClub]                      VARCHAR (30)  NULL,
    [MMSClubID]                   INT           NULL,
    [ReportingRegionID]           INT           NULL,
    [NetworkClubGMPath]           VARCHAR (100) NULL,
    [InsertedDateTime]            DATETIME      NULL,
    [UpdatedDateTime]             DATETIME      NULL,
    [dv_load_date_time]           DATETIME      NOT NULL,
    [dv_inserted_date_time]       DATETIME      NOT NULL,
    [dv_insert_user]              VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]        DATETIME      NULL,
    [dv_update_user]              VARCHAR (50)  NULL,
    [dv_batch_id]                 BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

