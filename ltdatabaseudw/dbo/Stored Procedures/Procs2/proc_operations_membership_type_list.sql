CREATE PROC [dbo].[proc_operations_membership_type_list] @membership_type_list [varchar](500),@list_table [varchar](500) AS
begin
set xact_abort on
set nocount on

declare @table_Name varchar(500),
        @Product_table_Name varchar(500),
        ---@list_table varchar(500),
        --@membership_type_list varchar(500),
		-- @membership_type_list varchar(500),
		@sql varchar(max),
        @sql_insert varchar(max),
		@NewLineChar as char(2) = char(13) + char(10),
		@all_memberships_excluding_founders_flag nvarchar(1),
        @all_memberships_excluding_house_account_flag nvarchar(1),
        @corporate_flex_memberships_flag nvarchar(1),
        @employee_memberships_flag nvarchar(1),
        @flexible_pass_memberships_flag nvarchar(1),
        @founders_type_memberships_flag nvarchar(1),
        @house_account_memberships_flag nvarchar(1),
        @investor_memberships_flag nvarchar(1),
        @my_health_check_memberships_flag nvarchar(1),
        @non_access_memberships_flag nvarchar(1),
        @pending_non_access_memberships_flag nvarchar(1),
        @short_term_memberships_flag nvarchar(1),
        @student_flex_memberships_flag nvarchar(1),
        @trade_out_memberships_flag nvarchar(1),
        @vip_memberships_flag nvarchar(1),
        @26_and_under_memberships_flag nvarchar(1),
        @ignore_prompt_flag nvarchar(1),
        @lt_health_memberships_flag nvarchar(1),
		@all_memberships_excluding_founders_flag_sql NVARCHAR(1000),
        @all_memberships_excluding_house_account_flag_sql NVARCHAR(1000),
        @corporate_flex_memberships_flag_sql NVARCHAR(1000),
        @employee_memberships_flag_sql NVARCHAR(1000),
        @flexible_pass_memberships_flag_sql NVARCHAR(1000),
        @founders_type_memberships_flag_sql NVARCHAR(1000),
        @house_account_memberships_flag_sql NVARCHAR(1000),
        @investor_memberships_flag_sql NVARCHAR(1000),
        @my_health_check_memberships_flag_sql NVARCHAR(1000),
        @non_access_memberships_flag_sql NVARCHAR(1000),
        @pending_non_access_memberships_flag_sql NVARCHAR(1000),
        @short_term_memberships_flag_sql NVARCHAR(1000),
        @student_flex_memberships_flag_sql NVARCHAR(1000),
        @trade_out_memberships_flag_sql NVARCHAR(1000),
        @vip_memberships_flag_sql NVARCHAR(1000),
        @26_and_under_memberships_flag_sql NVARCHAR(1000),
        @ignore_prompt_flag_sql NVARCHAR(1000),
        @lt_health_memberships_flag_sql NVARCHAR(1000)

       -- SET @membership_type_list ='All Memberships - Excluding Founders|All Memberships - Excluding House Account|Corporate Flex Memberships|Employee Memberships|Flexible Pass Memberships|Founders Type Memberships|House Account Memberships|Investor Memberships|myHealthCheck Memberships|Non-Access Memberships|Pending Non-Access Memberships|Short Term Memberships|Student Flex Memberships|Trade Out Memberships|VIP Memberships|26 and Under Memberships|< Ignore this prompt >|Life Time Health Memberships'
       -- SET @list_table ='temp_table'
	    SET @table_Name = '#'+@list_table
		SET @Product_table_Name = '#Membership_'+@list_table


exec proc_parse_pipe_list @Item=@membership_type_list,@list_table=@list_table

SET @all_memberships_excluding_founders_flag_sql = N'SELECT @all_memberships_excluding_founders_flag = CASE WHEN ''All Memberships - Excluding Founders'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @all_memberships_excluding_founders_flag_sql,N'@all_memberships_excluding_founders_flag NVARCHAR(1) OUTPUT',@all_memberships_excluding_founders_flag OUTPUT 
SET @all_memberships_excluding_house_account_flag_sql = N'SELECT @all_memberships_excluding_house_account_flag = CASE WHEN ''All Memberships - Excluding House Account'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @all_memberships_excluding_house_account_flag_sql,N'@all_memberships_excluding_house_account_flag NVARCHAR(50) OUTPUT',@all_memberships_excluding_house_account_flag OUTPUT  
SET @corporate_flex_memberships_flag_sql = N'SELECT @corporate_flex_memberships_flag = CASE WHEN ''Corporate Flex Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @corporate_flex_memberships_flag_sql,N'@corporate_flex_memberships_flag NVARCHAR(50) OUTPUT',@corporate_flex_memberships_flag OUTPUT  
SET @employee_memberships_flag_sql = N'SELECT @employee_memberships_flag = CASE WHEN ''Employee Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @employee_memberships_flag_sql,N'@employee_memberships_flag NVARCHAR(50) OUTPUT',@employee_memberships_flag OUTPUT  
SET @flexible_pass_memberships_flag_sql = N'SELECT @flexible_pass_memberships_flag = CASE WHEN ''Flexible Pass Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @flexible_pass_memberships_flag_sql,N'@flexible_pass_memberships_flag NVARCHAR(50) OUTPUT',@flexible_pass_memberships_flag OUTPUT  
SET @founders_type_memberships_flag_sql = N'SELECT @founders_type_memberships_flag = CASE WHEN ''Founders Type Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @founders_type_memberships_flag_sql,N'@founders_type_memberships_flag NVARCHAR(50) OUTPUT',@founders_type_memberships_flag OUTPUT  
SET @house_account_memberships_flag_sql = N'SELECT @house_account_memberships_flag = CASE WHEN ''House Account Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @house_account_memberships_flag_sql,N'@house_account_memberships_flag NVARCHAR(50) OUTPUT',@house_account_memberships_flag OUTPUT  
SET @investor_memberships_flag_sql = N'SELECT @investor_memberships_flag = CASE WHEN ''Investor Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @investor_memberships_flag_sql,N'@investor_memberships_flag NVARCHAR(50) OUTPUT',@investor_memberships_flag OUTPUT  
SET @my_health_check_memberships_flag_sql = N'SELECT @my_health_check_memberships_flag = CASE WHEN ''myHealthCheck Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @my_health_check_memberships_flag_sql,N'@my_health_check_memberships_flag NVARCHAR(50) OUTPUT',@my_health_check_memberships_flag OUTPUT  
SET @non_access_memberships_flag_sql = N'SELECT @non_access_memberships_flag = CASE WHEN ''Non-Access Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @non_access_memberships_flag_sql,N'@non_access_memberships_flag NVARCHAR(50) OUTPUT',@non_access_memberships_flag OUTPUT  
SET @pending_non_access_memberships_flag_sql = N'SELECT @pending_non_access_memberships_flag = CASE WHEN ''Pending Non-Access Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @pending_non_access_memberships_flag_sql,N'@pending_non_access_memberships_flag NVARCHAR(50) OUTPUT',@pending_non_access_memberships_flag OUTPUT  
SET @short_term_memberships_flag_sql = N'SELECT @short_term_memberships_flag = CASE WHEN ''Short Term Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
exec sp_executesql @short_term_memberships_flag_sql,N'@short_term_memberships_flag NVARCHAR(50) OUTPUT',@short_term_memberships_flag OUTPUT  
SET @student_flex_memberships_flag_sql = N'SELECT @student_flex_memberships_flag = CASE WHEN ''Student Flex Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @student_flex_memberships_flag_sql,N'@student_flex_memberships_flag NVARCHAR(50) OUTPUT',@student_flex_memberships_flag OUTPUT  
SET @trade_out_memberships_flag_sql = N'SELECT @trade_out_memberships_flag = CASE WHEN ''Trade Out Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @trade_out_memberships_flag_sql,N'@trade_out_memberships_flag NVARCHAR(50) OUTPUT',@trade_out_memberships_flag OUTPUT  
SET @vip_memberships_flag_sql = N'SELECT @vip_memberships_flag = CASE WHEN ''VIP Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @vip_memberships_flag_sql,N'@vip_memberships_flag NVARCHAR(50) OUTPUT',@vip_memberships_flag OUTPUT  
SET @26_and_under_memberships_flag_sql = N'SELECT @26_and_under_memberships_flag = CASE WHEN ''26 and Under Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @26_and_under_memberships_flag_sql,N'@26_and_under_memberships_flag NVARCHAR(50) OUTPUT',@26_and_under_memberships_flag OUTPUT  
SET @ignore_prompt_flag_sql = N'SELECT @ignore_prompt_flag = CASE WHEN ''< Ignore this prompt >'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @ignore_prompt_flag_sql,N'@ignore_prompt_flag NVARCHAR(50) OUTPUT',@ignore_prompt_flag OUTPUT  
SET @lt_health_memberships_flag_sql = N'SELECT @lt_health_memberships_flag = CASE WHEN ''Life Time Health Memberships'' IN (SELECT Item FROM '+@table_Name+') THEN ''Y'' ELSE ''N'' END'
EXEC sp_executesql @lt_health_memberships_flag_sql,N'@lt_health_memberships_flag NVARCHAR(50) OUTPUT',@lt_health_memberships_flag OUTPUT  


set @sql = 'if object_id(''tempdb..'+@Product_table_Name+''') is not null
          drop table ' + @Product_table_Name + @NewLineChar+' create table dbo.'+@Product_table_Name + ' (dim_mms_product_key varchar(500)  null ) with (location=user_db,heap)' +@NewLineChar

--print @sql
exec(@sql)

--PRINT @Product_table_Name

		  
set @sql_insert ='INSERT INTO '+@Product_table_Name+
          ' SELECT DISTINCT dim_mms_product_key
          FROM dim_mms_membership_type   
          WHERE  
          ((dim_mms_membership_type.attribute_founders_flag = ''N'' AND '''+@all_memberships_excluding_founders_flag+'''=''Y'')
          OR (dim_mms_membership_type.product_house_account_flag = ''N'' AND '''+@all_memberships_excluding_house_account_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_corporate_flex_flag = ''Y'' AND '''+@corporate_flex_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_employee_membership_flag = ''Y'' AND '''+@employee_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_flexible_pass_flag = ''Y'' AND '''+@flexible_pass_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_founders_flag = ''Y'' AND '''+@founders_type_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.product_house_account_flag = ''Y'' AND '''+@house_account_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.product_investor_flag = ''Y'' AND '''+@investor_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_my_health_check_flag = ''Y'' AND '''+@my_health_check_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_non_access_flag = ''Y'' AND '''+@non_access_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_pending_non_access_flag = ''Y'' AND '''+@pending_non_access_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_short_term_membership_flag = ''Y'' AND '''+@short_term_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_student_flex_flag = ''Y'' AND '''+@student_flex_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_trade_out_membership_flag = ''Y'' AND '''+@trade_out_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_vip_flag = ''Y'' AND '''+@vip_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_26_and_under_flag = ''Y'' AND '''+@26_and_under_memberships_flag+'''=''Y'')
          OR (dim_mms_membership_type.attribute_life_time_health_flag = ''Y'' AND '''+@lt_health_memberships_flag+'''=''Y'')
          OR ('''+@ignore_prompt_flag +'''=''Y''))'

--print @sql_insert
exec(@sql_insert)

	end
