CREATE PROC [dbo].[proc_dv_deleted_lt_bucks_category_items] AS
begin

set nocount on
set xact_abort on

declare @user varchar(100) = suser_sname()

update h_lt_bucks_category_items
set dv_deleted = 1,
    dv_updated_date_time = getdate(),
    dv_update_user = @user
where h_lt_bucks_category_items_id in (select h.h_lt_bucks_category_items_id
                                         from h_lt_bucks_category_items h
                                         left join stage_lt_bucks_categoryitems s on h.citem_id = s.citem_id
                                        where h.bk_hash not in ('-997','-998','-999')
                                          and s.citem_id is null
                                          and h.dv_deleted = 0)

update h_lt_bucks_category_items
set dv_deleted = 0,
    dv_updated_date_time = getdate(),
    dv_update_user = @user
where h_lt_bucks_category_items_id in (select h.h_lt_bucks_category_items_id
                                         from stage_lt_bucks_categoryitems s 
                                         join h_lt_bucks_category_items h on h.citem_id = s.citem_id
                                        where h.dv_deleted = 1)



end
