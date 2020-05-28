CREATE TABLE [dbo].[stage_athlinks_api_vw_AthleteNonMember] (
    [stage_athlinks_api_vw_AthleteNonMember_id] BIGINT         NOT NULL,
    [RacerID]                                   INT            NULL,
    [FName]                                     NVARCHAR (255) NULL,
    [LName]                                     NVARCHAR (255) NULL,
    [DisplayName]                               NVARCHAR (255) NULL,
    [Age]                                       INT            NULL,
    [Gender]                                    CHAR (1)       NULL,
    [City]                                      NVARCHAR (255) NULL,
    [StateProvID]                               NVARCHAR (255) NULL,
    [StateProvName]                             NVARCHAR (255) NULL,
    [StateProvAbbrev]                           NVARCHAR (255) NULL,
    [CountryID]                                 CHAR (2)       NULL,
    [CountryID3]                                CHAR (3)       NULL,
    [CountryName]                               NVARCHAR (255) NULL,
    [PhotoPath]                                 VARCHAR (255)  NULL,
    [JoinDate]                                  DATETIME       NULL,
    [Notes]                                     VARCHAR (255)  NULL,
    [OwnerID]                                   INT            NULL,
    [IsMember]                                  VARCHAR (1)    NULL,
    [ResultCount]                               INT            NULL,
    [CreateDate]                                DATETIME       NULL,
    [dv_batch_id]                               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

