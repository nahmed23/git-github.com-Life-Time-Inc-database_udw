CREATE TABLE [dbo].[stage_ec_MeasurementRecordings] (
    [stage_ec_MeasurementRecordings_id] BIGINT             NOT NULL,
    [MeasurementRecordingId]            VARCHAR (36)       NULL,
    [PartyId]                           INT                NULL,
    [ClubId]                            INT                NULL,
    [UserProgramStatusId]               VARCHAR (36)       NULL,
    [MeasureDate]                       DATETIMEOFFSET (7) NULL,
    [Notes]                             NVARCHAR (500)     NULL,
    [Source]                            NVARCHAR (100)     NULL,
    [Active]                            BIT                NULL,
    [Certified]                         BIT                NULL,
    [CreatedBy]                         INT                NULL,
    [CreatedDate]                       DATETIMEOFFSET (7) NULL,
    [ModifiedBy]                        INT                NULL,
    [ModifiedDate]                      DATETIMEOFFSET (7) NULL,
    [MetaData]                          NVARCHAR (4000)    NULL,
    [dv_batch_id]                       BIGINT             NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

