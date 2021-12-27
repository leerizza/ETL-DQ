/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with x as (
  select 
    ga_session_id, 
    user_id, 
    user_pseudo_id, 
    event_name, 
    action_type_cleaned, 
    user_first_timestamp_gmt7, 
    timestamp_gmt7, 
    date_gmt7, 
    time_gmt7, 
    hour_gmt7, 
    device_category_cleaned, 
    engagement_time_msec, 
    material_id, 
    module_id, 
    video_id, 
    video_inspirasi_id, 
    video_inspirasi_playslist_id, 
    quiz_id, 
    text_id, 
    open_reflection_id, 
    save_reflection_id, 
    post_test_result, 
    event_params_source, 
    event_params_page_location, 
    event_params_page_referrer, 
    event_params_page_path, 
    event_params_page_title, 
    event_params_product 
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
  "ga_session_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank ga_session_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(ga_session_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(ga_session_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
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
  "user_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank user_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(user_id, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(user_id, '')= '' then 'fail' end
      ) as bad_data 
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
  "user_pseudo_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank user_pseudo_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull (cast(user_pseudo_id as string), '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull (cast(user_pseudo_id as string), '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result3 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result4.criteria, 
  result4.metrics, 
  result4.total_data, 
  result4.good_data, 
  result4.bad_data, 
  cast(
    cast(result4.good_data * 100 as decimal)/ result4.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result4.bad_data * 100 as decimal)/ result4.total_data as numeric
  ) as percentage_bad_data, 
  "event_name" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_name' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_name, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_name, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result4 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result5.criteria, 
  result5.metrics, 
  result5.total_data, 
  result5.good_data, 
  result5.bad_data, 
  cast(
    cast(result5.good_data * 100 as decimal)/ result5.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result5.bad_data * 100 as decimal)/ result5.total_data as numeric
  ) as percentage_bad_data, 
  "action_type_cleaned" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank action_type_cleaned' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(action_type_cleaned, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(action_type_cleaned, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result5 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result6.criteria, 
  result6.metrics, 
  result6.total_data, 
  result6.good_data, 
  result6.bad_data, 
  cast(
    cast(result6.good_data * 100 as decimal)/ result6.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result6.bad_data * 100 as decimal)/ result6.total_data as numeric
  ) as percentage_bad_data, 
  "user_first_timestamp_gmt7" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank user_first_timestamp_gmt7' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(
            user_first_timestamp_gmt7 as string
          ), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(
            user_first_timestamp_gmt7 as string
          ), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result6 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result7.criteria, 
  result7.metrics, 
  result7.total_data, 
  result7.good_data, 
  result7.bad_data, 
  cast(
    cast(result7.good_data * 100 as decimal)/ result7.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result7.bad_data * 100 as decimal)/ result7.total_data as numeric
  ) as percentage_bad_data, 
  "timestamp_gmt7" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank timestamp_gmt7' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(timestamp_gmt7 as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(timestamp_gmt7 as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result7 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result8.criteria, 
  result8.metrics, 
  result8.total_data, 
  result8.good_data, 
  result8.bad_data, 
  cast(
    cast(result8.good_data * 100 as decimal)/ result8.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result8.bad_data * 100 as decimal)/ result8.total_data as numeric
  ) as percentage_bad_data, 
  "date_gmt7" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank date_gmt7' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(date_gmt7 as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(date_gmt7 as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result8 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result9.criteria, 
  result9.metrics, 
  result9.total_data, 
  result9.good_data, 
  result9.bad_data, 
  cast(
    cast(result9.good_data * 100 as decimal)/ result9.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result9.bad_data * 100 as decimal)/ result9.total_data as numeric
  ) as percentage_bad_data, 
  "time_gmt7" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank time_gmt7' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(cast(time_gmt7 as string), '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(cast(time_gmt7 as string), '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result9 

union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result10.criteria, 
  result10.metrics, 
  result10.total_data, 
  result10.good_data, 
  result10.bad_data, 
  cast(
    cast(
      result10.good_data * 100 as decimal
    )/ result10.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result10.bad_data * 100 as decimal)/ result10.total_data as numeric
  ) as percentage_bad_data, 
  "hour_gmt7" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank hour_gmt7' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(hour_gmt7 as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(hour_gmt7 as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result10 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result11.criteria, 
  result11.metrics, 
  result11.total_data, 
  result11.good_data, 
  result11.bad_data, 
  cast(
    cast(
      result11.good_data * 100 as decimal
    )/ result11.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result11.bad_data * 100 as decimal)/ result11.total_data as numeric
  ) as percentage_bad_data, 
  "device_category_cleaned" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank device_category_cleaned' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(device_category_cleaned, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(device_category_cleaned, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result11 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result12.criteria, 
  result12.metrics, 
  result12.total_data, 
  result12.good_data, 
  result12.bad_data, 
  cast(
    cast(
      result12.good_data * 100 as decimal
    )/ result12.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result12.bad_data * 100 as decimal)/ result12.total_data as numeric
  ) as percentage_bad_data, 
  "engagement_time_msec" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank engagement_time_msec' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(engagement_time_msec as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(engagement_time_msec as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result12 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result13.criteria, 
  result13.metrics, 
  result13.total_data, 
  result13.good_data, 
  result13.bad_data, 
  cast(
    cast(
      result13.good_data * 100 as decimal
    )/ result13.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result13.bad_data * 100 as decimal)/ result13.total_data as numeric
  ) as percentage_bad_data, 
  "material_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank material_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(material_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(material_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned IN ('open_text', 'play_video_material', 'open_quiz', 'open_reflection', 'complete_material_quiz', 'save_reflection', 'play_material_video', 'play_video')  
  ) result13 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result14.criteria, 
  result14.metrics, 
  result14.total_data, 
  result14.good_data, 
  result14.bad_data, 
  cast(
    cast(
      result14.good_data * 100 as decimal
    )/ result14.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result14.bad_data * 100 as decimal)/ result14.total_data as numeric
  ) as percentage_bad_data, 
  "module_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank module_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(module_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(module_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned IN ('open_text', 'play_video_material', 'open_quiz', 'open_reflection', 'complete_material_quiz', 'save_reflection', 'play_material_video', 'play_video')  
  ) result14 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result15.criteria, 
  result15.metrics, 
  result15.total_data, 
  result15.good_data, 
  result15.bad_data, 
  cast(
    cast(
      result15.good_data * 100 as decimal
    )/ result15.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result15.bad_data * 100 as decimal)/ result15.total_data as numeric
  ) as percentage_bad_data, 
  "video_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank video_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(video_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(video_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = "play_video"  
  ) result15 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result16.criteria, 
  result16.metrics, 
  result16.total_data, 
  result16.good_data, 
  result16.bad_data, 
  cast(
    cast(
      result16.good_data * 100 as decimal
    )/ result16.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result16.bad_data * 100 as decimal)/ result16.total_data as numeric
  ) as percentage_bad_data, 
  "video_inspirasi_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank video_inspirasi_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(video_inspirasi_id, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(video_inspirasi_id, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = "play_video_inspirasi"  
  ) result16 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result17.criteria, 
  result17.metrics, 
  result17.total_data, 
  result17.good_data, 
  result17.bad_data, 
  cast(
    cast(
      result17.good_data * 100 as decimal
    )/ result17.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result17.bad_data * 100 as decimal)/ result17.total_data as numeric
  ) as percentage_bad_data, 
  "video_inspirasi_playslist_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank video_inspirasi_playslist_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(
            video_inspirasi_playslist_id as string
          ), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(
            video_inspirasi_playslist_id as string
          ), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = "play_video_inspirasi"  
  ) result17 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result18.criteria, 
  result18.metrics, 
  result18.total_data, 
  result18.good_data, 
  result18.bad_data, 
  cast(
    cast(
      result18.good_data * 100 as decimal
    )/ result18.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result18.bad_data * 100 as decimal)/ result18.total_data as numeric
  ) as percentage_bad_data, 
  "quiz_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank quiz_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(quiz_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(quiz_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = 'open_quiz'  
  ) result18 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result19.criteria, 
  result19.metrics, 
  result19.total_data, 
  result19.good_data, 
  result19.bad_data, 
  cast(
    cast(
      result19.good_data * 100 as decimal
    )/ result19.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result19.bad_data * 100 as decimal)/ result19.total_data as numeric
  ) as percentage_bad_data, 
  "text_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank text_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(text_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(text_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = 'open_text'  
  ) result19 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result20.criteria, 
  result20.metrics, 
  result20.total_data, 
  result20.good_data, 
  result20.bad_data, 
  cast(
    cast(
      result20.good_data * 100 as decimal
    )/ result20.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result20.bad_data * 100 as decimal)/ result20.total_data as numeric
  ) as percentage_bad_data, 
  "open_reflection_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank open_reflection_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(open_reflection_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(open_reflection_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = "open_reflection"  
  ) result20 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result21.criteria, 
  result21.metrics, 
  result21.total_data, 
  result21.good_data, 
  result21.bad_data, 
  cast(
    cast(
      result21.good_data * 100 as decimal
    )/ result21.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result21.bad_data * 100 as decimal)/ result21.total_data as numeric
  ) as percentage_bad_data, 
  "save_reflection_id" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank save_reflection_id' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(
          cast(save_reflection_id as string), 
          ''
        )<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(
          cast(save_reflection_id as string), 
          ''
        )= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = 'save_reflection'  
  ) result21 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result22.criteria, 
  result22.metrics, 
  result22.total_data, 
  result22.good_data, 
  result22.bad_data, 
  cast(
    cast(
      result22.good_data * 100 as decimal
    )/ result22.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result22.bad_data * 100 as decimal)/ result22.total_data as numeric
  ) as percentage_bad_data, 
  "post_test_result" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank post_test_result' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(post_test_result, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(post_test_result, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
    where action_type_cleaned = 'finished_post_test'  
  ) result22 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result23.criteria, 
  result23.metrics, 
  result23.total_data, 
  result23.good_data, 
  result23.bad_data, 
  cast(
    cast(
      result23.good_data * 100 as decimal
    )/ result23.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result23.bad_data * 100 as decimal)/ result23.total_data as numeric
  ) as percentage_bad_data, 
  "event_params_source" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_params_source' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_params_source, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_params_source, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result23 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result24.criteria, 
  result24.metrics, 
  result24.total_data, 
  result24.good_data, 
  result24.bad_data, 
  cast(
    cast(
      result24.good_data * 100 as decimal
    )/ result24.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result24.bad_data * 100 as decimal)/ result24.total_data as numeric
  ) as percentage_bad_data, 
  "event_params_page_location" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_params_page_location' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_params_page_location, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_params_page_location, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result24 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result25.criteria, 
  result25.metrics, 
  result25.total_data, 
  result25.good_data, 
  result25.bad_data, 
  cast(
    cast(
      result25.good_data * 100 as decimal
    )/ result25.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result25.bad_data * 100 as decimal)/ result25.total_data as numeric
  ) as percentage_bad_data, 
  "event_params_page_referrer" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_params_page_referrer' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_params_page_referrer, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_params_page_referrer, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result25 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result26.criteria, 
  result26.metrics, 
  result26.total_data, 
  result26.good_data, 
  result26.bad_data, 
  cast(
    cast(
      result26.good_data * 100 as decimal
    )/ result26.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result26.bad_data * 100 as decimal)/ result26.total_data as numeric
  ) as percentage_bad_data, 
  "event_params_page_path" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_params_page_path' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_params_page_path, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_params_page_path, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result26 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result27.criteria, 
  result27.metrics, 
  result27.total_data, 
  result27.good_data, 
  result27.bad_data, 
  cast(
    cast(
      result27.good_data * 100 as decimal
    )/ result27.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result27.bad_data * 100 as decimal)/ result27.total_data as numeric
  ) as percentage_bad_data, 
  "event_params_page_title" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_params_page_title' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_params_page_title, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_params_page_title, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result27 
union all 
select 
  datetime(
    current_timestamp(), 
    "Asia/Jakarta"
  ) as time_execution, 
  result28.criteria, 
  result28.metrics, 
  result28.total_data, 
  result28.good_data, 
  result28.bad_data, 
  cast(
    cast(
      result28.good_data * 100 as decimal
    )/ result28.total_data as numeric
  ) as percentage_good_data, 
  cast(
    cast(result28.bad_data * 100 as decimal)/ result28.total_data as numeric
  ) as percentage_bad_data, 
  "event_params_product" field_name_checking 
from 
  (
    select 
      'Completeness' criteria, 
      'field mandatory is null/blank event_params_product' metrics, 
      count(ga_session_id) total_data, 
      count(
        case when ifnull(event_params_product, '')<> '' then 'pass' end
      ) as good_data, 
      count(
        case when ifnull(event_params_product, '')= '' then 'fail' end
      ) as bad_data 
    from 
      x
  ) result28
  