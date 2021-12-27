
{{ config(materialized='table') }}

select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result.criteria, 
  result.metrics, 
  result.total_data, 
  result.good_data, 
  result.bad_data, 
  cast(
    cast(result.good_data * 100 as decimal)/ result.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result.bad_data * 100 as decimal)/ result.total_data as numeric
  ) as percentage_bad_data, 
  "updated_at" field_name_checking 
from 
  (
    select 
      'Timeliness' criteria, 
      'freshness data Day - 1 on staging table' metrics, 
      count(updated_at) total_data, 
      count(
        case when updated_at in (
          select 
            updated_at 
          from 
            `playground-325606.dataset_dbt.event_tracker_belajar`
          where 
            date(
              current_date()
            ) < DATE_SUB(
              current_date(), 
              interval 1 day
            )
        ) then 'true' end
      ) as good_data, 
      count(
        case when updated_at not in (
          select 
            updated_at 
          from 
            `playground-325606.dataset_dbt.event_tracker_belajar`
          where 
            date(
              current_date()
            ) < DATE_SUB(
              current_date(), 
              interval 1 day
            )
        ) then 'false' end
      ) as bad_data, 
    from 
      `playground-325606.dataset_dbt.event_tracker_belajar`
  ) result
