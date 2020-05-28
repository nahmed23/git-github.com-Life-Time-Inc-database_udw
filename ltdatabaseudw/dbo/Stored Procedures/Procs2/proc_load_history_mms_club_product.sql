CREATE PROC [dbo].[proc_load_history_mms_club_product] AS
begin

set nocount on
set xact_abort on

/*Select the records from [dbo].[MMSClubProduct] to be staged and inserted into the dv tables*/

if object_id('tempdb.dbo.#stage_mms_clubproduct_history') is not null drop table #stage_mms_clubproduct_history
create table dbo.#stage_mms_clubproduct_history with (location=user_db, distribution = hash(ClubProductID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubProductID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValCommissionableID as varchar(500)),'z#@$k%&P'))),2)   l_mms_club_Product_hash,
        convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(Price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(SoldInPK as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,UpdatedDateTime,120),'z#@$k%&P'))),2)  s_mms_club_Product_hash ,

        row_number() over(partition by ClubProductID order by x.update_insert_date) rank2,
		*
  from (select row_number() over(partition by ClubProductID,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, isnull(MMSUpdatedDateTime,InsertedDateTime)) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   else dateadd(hh, 27, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   end
                   order by [MMSClubProductKey] desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, isnull(MMSUpdatedDateTime,InsertedDateTime)) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   else dateadd(hh, 27, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
               end update_insert_date,
               *
          from stage_mms_clubproduct_history) x
 where rank1 = 1					
                              
/* Create the h records.*/
/* dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.h_mms_club_product(
	   bk_hash,
       club_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select bk_hash,
              #stage_mms_clubproduct_history.ClubProductID ,
               isnull(cast(#stage_mms_clubproduct_history.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_clubproduct_history 
         where rank2 = 1) x
         		
/* Create the l records.*/
/* Calculate dv_load_date_time*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.l_mms_club_product (
		 bk_hash,
       club_product_id,
       club_id,
       product_id,
       val_commissionable_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_clubproduct_history.bk_hash,
       #stage_mms_clubproduct_history.ClubProductID club_product_id,
       #stage_mms_clubproduct_history.ClubID,
       #stage_mms_clubproduct_history.ProductID,
      #stage_mms_clubproduct_history.ValCommissionableID,
              case when #stage_mms_clubproduct_history.rank2 = 1 then
                         case when #stage_mms_clubproduct_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_clubproduct_history.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_clubproduct_history.rank2 = 1 then
                         case when #stage_mms_clubproduct_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_clubproduct_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)) in (0, 1) then 
					replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_clubproduct_history.l_mms_club_Product_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
           from dbo.#stage_mms_clubproduct_history
          left join dbo.#stage_mms_clubproduct_history prior
            on #stage_mms_clubproduct_history.ClubProductID = prior.ClubProductID
           and #stage_mms_clubproduct_history.rank2 = prior.rank2 + 1
         where #stage_mms_clubproduct_history.l_mms_club_Product_hash != isnull(prior.l_mms_club_Product_hash, ''))x

/* Create the s records.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.s_mms_club_product (
      
	    bk_hash,
       club_product_id,
       price,
       inserted_date_time,
       sold_in_pk,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select #stage_mms_clubproduct_history.bk_hash,
       #stage_mms_clubproduct_history.ClubProductID,
       #stage_mms_clubproduct_history.price,
       #stage_mms_clubproduct_history.MMSInsertedDateTime inserted_date_time,
       #stage_mms_clubproduct_history.SoldInPK,
       #stage_mms_clubproduct_history.MMSUpdatedDateTime updated_date_time,
               case when #stage_mms_clubproduct_history.rank2 = 1 then
                         case when #stage_mms_clubproduct_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_clubproduct_history.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_clubproduct_history.rank2 = 1 then
                         case when #stage_mms_clubproduct_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_clubproduct_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)) in (0, 1) 
					then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_clubproduct_history.MMSUpdatedDateTime,#stage_mms_clubproduct_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_clubproduct_history.s_mms_club_Product_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_clubproduct_history
          left join dbo.#stage_mms_clubproduct_history prior
            on #stage_mms_clubproduct_history.ClubProductID = prior.ClubProductID
           and #stage_mms_clubproduct_history.rank2 = prior.rank2 + 1
         where #stage_mms_clubproduct_history.s_mms_club_Product_hash != isnull(prior.s_mms_club_Product_hash, ''))x


/* Populate the pit table*/
truncate table dbo.p_mms_club_product						
exec dbo.proc_p_mms_club_product @current_dv_batch_id = -1 

end
