CREATE PROC [dbo].[proc_util_add_dv_columns] @table_name [varchar](256) AS
begin

  set xact_abort on
  set nocount on

  declare @stmt nvarchar(4000)
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_greatest_satellite_date_time datetime null'
  if substring(@table_name, 1, 2) in ('p_','q_')
    exec sp_executesql @stmt

  set @stmt = 'alter table dbo.' + @table_name + ' add dv_next_greatest_satellite_date_time datetime null'
  if substring(@table_name, 1, 2) in ('p_','q_')
    exec sp_executesql @stmt

  set @stmt = 'alter table dbo.' + @table_name + ' add dv_load_date_time datetime not null'
  exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_load_end_date_time datetime not null'
  if substring(@table_name, 1, 2) in ('r_','p_','b_','q_')
    exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_batch_id bigint not null'
  exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_r_load_source_id bigint not null'
  if substring(@table_name,1,2) in ('h_','l_','r_','s_','b_')
    exec sp_executesql @stmt
  
--  Default constraints are not supported in Azure DW
--  set @stmt = 'alter table dbo.' + @table_name + ' add dv_inserted_date_time datetime default getdate() not null'
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_inserted_date_time datetime not null'
  exec sp_executesql @stmt
  
--  Default constraints are not supported in Azure DW
--  set @stmt = 'alter table dbo.' + @table_name + ' add dv_insert_user varchar(50) default suser_sname() not null'
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_insert_user varchar(50) not null'
  exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_updated_date_time datetime null'
  exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_update_user varchar(50) null'
  exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_hash char(32) not null'
  if substring(@table_name, 1, 2) in ('l_','r_','s_')
    exec sp_executesql @stmt
  
  set @stmt = 'alter table dbo.' + @table_name + ' add dv_deleted bit default 0 not null'
  if substring(@table_name, 1, 2) in ('h_')
    exec sp_executesql @stmt

  set @stmt = 'alter table dbo.' + @table_name + ' add dv_first_in_key_series bit null'
  if substring(@table_name, 1, 2) in ('p_','q_')
    exec sp_executesql @stmt

end
