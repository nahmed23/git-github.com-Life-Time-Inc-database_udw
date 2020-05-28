CREATE ROLE [Sandbox-EBI]
    AUTHORIZATION [dbo];


GO
EXECUTE sp_addrolemember @rolename = N'Sandbox-EBI', @membername = N'SEC-DB-UDW-Developers';


GO
EXECUTE sp_addrolemember @rolename = N'Sandbox-EBI', @membername = N'SEC-DB-UDW-Elevated';


GO
EXECUTE sp_addrolemember @rolename = N'Sandbox-EBI', @membername = N'JobSchedulerUser';


GO
EXECUTE sp_addrolemember @rolename = N'Sandbox-EBI', @membername = N'DataScienceUser';

