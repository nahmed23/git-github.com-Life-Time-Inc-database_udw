CREATE PROC [dbo].[proc_dim_employee_role] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       -1 as current_dv_batch_id
  from dbo.dim_employee_role

if object_id('tempdb..#val_employee_role_id') is not null drop table #val_employee_role_id
create table dbo.#val_employee_role_id with(distribution=hash(val_employee_role_id), location=user_db, heap) as
select val_employee_role_id,
       rank() over (order by val_employee_role_id) r
from (select val_employee_role_id
      from r_mms_val_employee_role
     join #dv_batch_id
       on r_mms_val_employee_role.dv_batch_id > #dv_batch_id.max_dv_batch_id
       or r_mms_val_employee_role.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where r_mms_val_employee_role.dv_load_end_date_time = 'Dec 31, 9999'
      union
      select r_mms_val_employee_role.val_employee_role_id
      from r_mms_val_employee_role
      join p_mms_department
        on r_mms_val_employee_role.department_id = p_mms_department.department_id
		and p_mms_department.dv_load_end_date_time = 'Dec 31, 9999'
       join #dv_batch_id
        on p_mms_department.dv_batch_id > #dv_batch_id.max_dv_batch_id
       or p_mms_department.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_mms_department.dv_load_end_date_time = 'Dec 31, 9999') x

	
	      

--dim_employee_role
if object_id('tempdb..#r_mms_val_employee_role') is not null drop table #r_mms_val_employee_role
create table dbo.#r_mms_val_employee_role with(distribution=hash(dim_employee_role_key), location=user_db, heap) as
select r_mms_val_employee_role.bk_hash dim_employee_role_key,
        r_mms_val_employee_role.val_employee_role_id val_employee_role_id,
   		r_mms_val_employee_role.description role_name,
		case when r_mms_val_employee_role.bk_hash in ('-997','-998','-999') then 'N'
       when r_mms_val_employee_role.commissionable_flag = 1 then 'Y'
       else 'N'
       end
	   commissionable_flag,
	   r_mms_val_employee_role.r_mms_val_employee_role_id,
		r_mms_val_employee_role.dv_batch_id,
		r_mms_val_employee_role.dv_load_date_time,
        r_mms_val_employee_role.dv_load_end_date_time,
		#val_employee_role_id.r
   		from  r_mms_val_employee_role 
		join #val_employee_role_id on
		r_mms_val_employee_role.val_employee_role_id =  #val_employee_role_id.val_employee_role_id
		where r_mms_val_employee_role.dv_load_end_date_time = 'Dec 31, 9999'

		
if object_id('tempdb..#p_mms_department') is not null drop table #p_mms_department
create table dbo.#p_mms_department with(distribution=hash(dim_employee_role_key), location=user_db, heap) as
select  case when r_mms_val_employee_role.val_employee_role_id is null then '-998'
		else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(r_mms_val_employee_role.val_employee_role_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_employee_role_key,
        s_mms_department.description mms_department_name,
		p_mms_department.p_mms_department_id,
		p_mms_department.dv_batch_id,
		p_mms_department.dv_load_date_time,
        p_mms_department.dv_load_end_date_time
		from 
		r_mms_val_employee_role
		join p_mms_department on 
		p_mms_department.department_id = r_mms_val_employee_role.department_id
		join s_mms_department on
		s_mms_department.s_mms_department_id  = p_mms_department.s_mms_department_id
		join #val_employee_role_id on 
		r_mms_val_employee_role.val_employee_role_id =  #val_employee_role_id.val_employee_role_id
		and r_mms_val_employee_role.dv_load_end_date_time = 'Dec 31, 9999'
		where p_mms_department.dv_load_end_date_time = 'Dec 31, 9999' 
		

		
--delete and re-insert
declare @start int, @end int
set @start = 1
set @end = (select max(r) from #val_employee_role_id)		

while @start <= @end
begin
-- do as a single transaction
--   delete records from the dim table that exist
--   insert records from records from current and missing batches
    begin tran
      delete dbo.dim_employee_role
       where val_employee_role_id in (select val_employee_role_id from #val_employee_role_id where r >= @start and r < @start+60000000)
	   
	insert dbo.dim_employee_role(
    dim_employee_role_key,
    val_employee_role_id,
    role_name,
    mms_department_name,
    commissionable_flag,
    --EffectiveDate,
    --ExpirationDate,
    --ActiveInd,
	dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user)
select 	#r_mms_val_employee_role.dim_employee_role_key,
		#r_mms_val_employee_role.val_employee_role_id,
		isnull(#r_mms_val_employee_role.role_name,'') role_name,
		isnull(#p_mms_department.mms_department_name,'') mms_department_name,
		isnull(#r_mms_val_employee_role.commissionable_flag,'') commissionable_flag,
	  --#r_mms_val_employee_role.r_mms_val_employee_role_id ,
	  --#p_mms_department.p_mms_department_id ,
			case when #r_mms_val_employee_role.dv_load_date_time >  isnull(#p_mms_department.dv_load_date_time,'')
	                  then #r_mms_val_employee_role.dv_load_date_time
					  else isnull(#p_mms_department.dv_load_date_time,'')  end dv_load_date_time,
            case when #r_mms_val_employee_role.dv_load_end_date_time >  isnull(#p_mms_department.dv_load_end_date_time,'')
	                 then #r_mms_val_employee_role.dv_load_end_date_time
					 else isnull(#p_mms_department.dv_load_end_date_time,'')  end dv_load_end_date_time,	
			case when #r_mms_val_employee_role.dv_batch_id >  isnull(#p_mms_department.dv_batch_id,'-2')
	                 then #r_mms_val_employee_role.dv_batch_id
					   else isnull(#p_mms_department.dv_batch_id,'')  end dv_batch_id,
             getdate(),
             suser_sname()				  
	  from 
	  #r_mms_val_employee_role 
	   Left join #p_mms_department on 
	  #p_mms_department.dim_employee_role_key = #r_mms_val_employee_role.dim_employee_role_key
	  where #r_mms_val_employee_role.r >= @start
	  and #r_mms_val_employee_role.r < @start+60000000
    commit tran

    set @start = @start+60000000
end
end
