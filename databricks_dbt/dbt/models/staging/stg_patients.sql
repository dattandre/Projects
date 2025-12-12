

SELECT
    *,
    null as updated_at,
    id as patient_id
from {{ source( 'test_data', 'patients' ) }}