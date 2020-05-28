CREATE TABLE [dbo].[stage_hash_ec_MeasurementRecordings] (
    [stage_hash_ec_MeasurementRecordings_id] BIGINT             IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)          NOT NULL,
    [MeasurementRecordingId]                 VARCHAR (36)       NULL,
    [PartyId]                                INT                NULL,
    [ClubId]                                 INT                NULL,
    [UserProgramStatusId]                    VARCHAR (36)       NULL,
    [MeasureDate]                            DATETIMEOFFSET (7) NULL,
    [Notes]                                  NVARCHAR (500)     NULL,
    [Source]                                 NVARCHAR (100)     NULL,
    [Active]                                 BIT                NULL,
    [Certified]                              BIT                NULL,
    [CreatedBy]                              INT                NULL,
    [CreatedDate]                            DATETIMEOFFSET (7) NULL,
    [ModifiedBy]                             INT                NULL,
    [ModifiedDate]                           DATETIMEOFFSET (7) NULL,
    [MetaData]                               NVARCHAR (4000)    NULL,
    [dv_load_date_time]                      DATETIME           NOT NULL,
    [dv_updated_date_time]                   DATETIME           NULL,
    [dv_update_user]                         VARCHAR (50)       NULL,
    [dv_batch_id]                            BIGINT             NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

