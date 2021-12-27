

  create or replace table `playground-325606`.`dataset_dbt`.`dqa_uniqueness`
  
  
  OPTIONS()
  as (
    /*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



with x as (

    select distinct 
date_gmt7,
time_gmt7,
hour_gmt7, 
timestamp_gmt7,
event_name,
ga_session_id,
user_id,
event_params_page_title,
event_params_product,
action_type_cleaned, count(1) total
FROM `playground-325606.dataset_dbt.event_tracker_belajar`
group by date_gmt7,
time_gmt7,
hour_gmt7, 
timestamp_gmt7,
event_name,
ga_session_id,
user_id,
event_params_page_title,
event_params_product,
action_type_cleaned

)

select datetime(current_timestamp(), "Asia/Jakarta") as time_execution, result.criteria, result.metrics, result.total_data, result.good_data, result.bad_data, cast(cast(result.good_data * 100 as decimal)/result.total_data as numeric) as percentage_good_data, cast(cast(result.bad_data * 100 as decimal)/result.total_data as numeric) as percentage_bad_data, "date_gmt7, time_gmt7, hour_gmt7, timestamp_gmt7, event_name, ga_session_id, user_id, event_params_page_title, event_params_product, action_type_cleaned" field_name_checking
from (
select 'Uniqueness' criteria,
'duplicate in rows' metrics,
count(x.total ) as total_data,
count(
    case 
    when (x.total = 1) then 'pass' end ) as good_data,
count(
    case
    when (x.total > 1) then 'fail' end ) as bad_data        
from x
)result


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
  );
    