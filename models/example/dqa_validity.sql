
{{ config(materialized='table') }}

with x as (
  select 
    user_id,
    ga_session_id,
    video_inspirasi_id  
  FROM 
    `playground-325606.dataset_dbt.event_tracker_belajar`
)
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result1.criteria, 
  result1.metrics, 
  result1.total_data, 
  result1.good_data, 
  result1.bad_data, 
  cast(
    cast(result1.good_data * 100 as decimal)/ result1.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result1.bad_data * 100 as decimal)/ result1.total_data as numeric
  ) as percentage_bad_data, 
  "user_id" field_name_checking 
from 
  (
    select 
      'Validity' criteria, 
      'not valid format, syntax or length user_id' metrics, 
      count(x.user_id) total_data, 
      count(
        case when length(x.user_id) = 10 then 'pass' end
      ) as good_data, 
      count(
        case when length(x.user_id) < 10 
        or length(x.user_id) > 10 then 'fail' end
      ) as bad_data, 
    from 
      x
  ) result1 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result2.criteria, 
  result2.metrics, 
  result2.total_data, 
  result2.good_data, 
  result2.bad_data, 
  cast(
    cast(result2.good_data * 100 as decimal)/ result2.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result2.bad_data * 100 as decimal)/ result2.total_data as numeric
  ) as percentage_bad_data, 
  "ga_session_id" field_name_checking 
from 
  (
    select 
      'Validity' criteria, 
      'not valid format, syntax or length ga_session_id' metrics, 
      count(x.ga_session_id) total_data, 
      count(
        case when length(
          cast(x.ga_session_id as string)
        ) = 10 then 'pass' end
      ) as good_data, 
      count(
        case when length(
          cast(x.ga_session_id as string)
        )< 10 
        or length(
          cast(x.ga_session_id as string)
        ) > 10 then 'fail' end
      ) as bad_data, 
    from 
      x
  ) result2 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result3.criteria, 
  result3.metrics, 
  result3.total_data, 
  result3.good_data, 
  result3.bad_data, 
  cast(
    cast(result3.good_data * 100 as decimal)/ result3.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result3.bad_data * 100 as decimal)/ result3.total_data as numeric
  ) as percentage_bad_data, 
  "video_inspirasi_id" field_name_checking 
from 
  (
    select 
      'Validity' criteria, 
      'not valid format, syntax or length video_inspirasi_id' metrics, 
      count(x.video_inspirasi_id) total_data, 
      count(
        case when length(x.video_inspirasi_id) = 11 then 'pass' end
      ) as good_data, 
      count(
        case when length(x.video_inspirasi_id) < 11 
        or length(x.video_inspirasi_id) > 11 then 'fail' end
      ) as bad_data, 
    from 
      x
  ) result3
