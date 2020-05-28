CREATE PROC [dbo].[proc_dim_employee_certification] AS
begin

set nocount on
set xact_abort on

declare @delimiter varchar(2) = ';'

if object_id('tempdb..#process') is not null drop table #process
create table #process with (distribution = hash(dim_employee_key)) as
select dim_employee_key,
       certifications,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
from d_workday_employee

truncate table dim_employee_certification

declare @s int = 1
declare @e int = (select max(len(certifications) - len(replace(certifications,@delimiter,''))+1) certification_delim_count from #process)
while @s <= @e
begin

    if object_id('tempdb..#tp') is not null drop table #tp
    create table #tp with (distribution = hash(dim_employee_key)) as
    select dim_employee_key,
           certifications,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           getdate() dv_inserted_date_time,
           suser_sname() dv_insert_user
      from #process
     where CHARINDEX(@delimiter,certifications) = 0

    --insert records with only a single job where no more parsing is required
    insert into dim_employee_certification (
        dim_employee_key,
        certification,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_inserted_date_time,
        dv_insert_user
        )
    select dim_employee_key,
           ltrim(rtrim(certifications)),
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           dv_inserted_date_time,
           dv_insert_user
      from #tp
     where CHARINDEX(@delimiter,certifications) = 0

     --stop processing records that have been parsed
     delete from #process where CHARINDEX(@delimiter,certifications) = 0

     --insert records with more than 1 job where additional parsing is required
    insert into dim_employee_certification (
        dim_employee_key,
        certification,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_inserted_date_time,
        dv_insert_user
        )
    select dim_employee_key,
           ltrim(rtrim(substring(certifications,1,charindex(@delimiter,certifications)-1))),
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           getdate(),
           suser_sname()
      from #process

    --remove the inserted job element from the string
    update #process
       set certifications = ltrim(rtrim(right(certifications,len(certifications)-charindex(@delimiter,certifications))))

    --repeat til end
    set @s = @s + 1
end

--delete out blank records due to the "random" commas
delete from dim_employee_certification where certification = ''

--jobs can be duplicated within the strings, find duplicates
if object_id('tempdb..#dedupe') is not null drop table #dedupe
create table #dedupe with (distribution = round_robin) as
select dim_employee_certification_id
from dim_employee_certification
join (select dim_employee_key, certification, min(dim_employee_certification_id) min_id from dim_employee_certification group by dim_employee_key, certification) dedupe
  on dim_employee_certification.dim_employee_key = dedupe.dim_employee_key and dim_employee_certification.certification = dedupe.certification and dim_employee_certification.dim_employee_certification_id > dedupe.min_id

--remove duplicates
delete dim_employee_certification
where dim_employee_certification_id in (select dim_employee_certification_id from #dedupe)


  
drop table #process
drop table #dedupe
end
