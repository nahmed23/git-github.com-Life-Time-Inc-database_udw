CREATE ROLE [Sandbox-Analysts-Elevated]
    AUTHORIZATION [dbo];


GO
EXECUTE sp_addrolemember @rolename = N'Sandbox-Analysts-Elevated', @membername = N'Alteryxsvc';


GO
EXECUTE sp_addrolemember @rolename = N'Sandbox-Analysts-Elevated', @membername = N'SEC-DB-UDW-Analysts-Elevated';

