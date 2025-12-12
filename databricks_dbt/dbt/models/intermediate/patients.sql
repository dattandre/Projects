
{{ 
    config(
        materialized='table'
    )
}}

with 

pt as ( select * from {{ ref( 'stg_patients' ) }} )

select
    *
    
from pt

qualify 1 = row_number() over (
    partition by patient_id 
    order by updated_at desc
)