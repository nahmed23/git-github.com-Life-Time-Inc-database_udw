﻿CREATE TABLE [dbo].[stage_mms_MembershipType] (
    [stage_mms_MembershipType_id]     BIGINT        NOT NULL,
    [MembershipTypeID]                INT           NULL,
    [ProductID]                       INT           NULL,
    [AssessDueFlag]                   BIT           NULL,
    [ValMembershipTypeGroupID]        INT           NULL,
    [ValCheckInGroupID]               INT           NULL,
    [InsertedDateTime]                DATETIME      NULL,
    [ValMembershipTypeFamilyStatusID] INT           NULL,
    [ShortTermMembershipFlag]         BIT           NULL,
    [ValEnrollmentTypeID]             INT           NULL,
    [ValUnitTypeID]                   INT           NULL,
    [MaxUnitType]                     INT           NULL,
    [MemberCardDesignID]              INT           NULL,
    [ExpressMembershipFlag]           BIT           NULL,
    [UpdatedDateTime]                 DATETIME      NULL,
    [ValWelcomeKitTypeID]             INT           NULL,
    [DisplayName]                     VARCHAR (50)  NULL,
    [AssessJrMemberDuesFlag]          BIT           NULL,
    [WaiveAdminFeeFlag]               BIT           NULL,
    [GTASigOverride]                  VARCHAR (100) NULL,
    [AllowPartnerProgramFlag]         BIT           NULL,
    [MinUnitType]                     INT           NULL,
    [MinPrimaryAge]                   INT           NULL,
    [ValPricingMethodID]              INT           NULL,
    [WaiveEnrollmentFeeFlag]          BIT           NULL,
    [WaiveLateFeeFlag]                BIT           NULL,
    [SuppressMembershipCardFlag]      BIT           NULL,
    [ValPricingRuleID]                INT           NULL,
    [ValRestrictedGroupID]            INT           NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
