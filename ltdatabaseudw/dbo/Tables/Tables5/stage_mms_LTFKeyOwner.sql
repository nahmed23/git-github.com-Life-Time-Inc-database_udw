﻿CREATE TABLE [dbo].[stage_mms_LTFKeyOwner] (
    [stage_mms_LTFKeyOwner_id] BIGINT        NOT NULL,
    [LTFKeyOwnerID]            INT           NULL,
    [PartyID]                  INT           NULL,
    [LTFKeyID]                 INT           NULL,
    [KeyPriority]              INT           NULL,
    [FromDate]                 DATETIME      NULL,
    [ThruDate]                 DATETIME      NULL,
    [FromTime]                 VARCHAR (15)  NULL,
    [ThruTime]                 VARCHAR (15)  NULL,
    [UsageCount]               INT           NULL,
    [UsageLimit]               INT           NULL,
    [AcquisitionID]            VARCHAR (50)  NULL,
    [ValAcquisitionTypeID]     INT           NULL,
    [ValOwnershipTypeID]       INT           NULL,
    [InsertedDateTime]         DATETIME      NULL,
    [UpdatedDateTime]          DATETIME      NULL,
    [DisplayName]              VARCHAR (200) NULL,
    [LTFKeyAcquisitionID]      INT           NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

