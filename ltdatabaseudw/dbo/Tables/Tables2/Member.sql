﻿CREATE TABLE [dbo].[Member] (
    [MemberID]               INT           NOT NULL,
    [MembershipID]           INT           NULL,
    [EmployerID]             INT           NULL,
    [FirstName]              VARCHAR (50)  NULL,
    [MiddleName]             VARCHAR (25)  NULL,
    [LastName]               VARCHAR (50)  NULL,
    [DOB]                    DATETIME      NULL,
    [Gender]                 CHAR (1)      NULL,
    [ssn]                    VARCHAR (9)   NULL,
    [ActiveFlag]             BIT           NOT NULL,
    [HasMessageFlag]         BIT           NOT NULL,
    [JoinDate]               DATETIME      NULL,
    [Comment]                VARCHAR (250) NULL,
    [ValMemberTypeID]        TINYINT       NOT NULL,
    [InsertedDateTime]       DATETIME      NULL,
    [ValNamePrefixID]        TINYINT       NULL,
    [ValNameSuffixID]        TINYINT       NULL,
    [EmailAddress]           VARCHAR (140) NULL,
    [CreditCardAccountID]    INT           NULL,
    [ChargeToaccountFlag]    BIT           NULL,
    [CWMedicaNumber]         VARCHAR (16)  NULL,
    [CWEnrollmentDate]       DATETIME      NULL,
    [CWProgramEnrolledFlag]  BIT           NULL,
    [MIPUpdatedDateTime]     DATETIME      NULL,
    [SiebelRow_ID]           VARCHAR (15)  NULL,
    [UpdatedDateTime]        DATETIME      NULL,
    [PhotoDeleteDateTime]    DATETIME      NULL,
    [Salesforce_Prospect_ID] VARCHAR (18)  NULL,
    [MemberToken]            BINARY (20)   NULL,
    [Party_ID]               INT           NULL,
    [LastUpdatedEmployeeID]  INT           NULL,
    [Salesforce_Contact_ID]  VARCHAR (18)  NULL,
    [AssessJrMemberDuesFlag] BIT           NULL,
    [CRMContactID]           VARCHAR (36)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([MemberID]));
