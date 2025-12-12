create schema if not exists raw;

-- patients
create or replace table raw.patients as select * from 'src_data/patients.csv';

-- orders
CREATE or replace table raw.orders as select * from 'src_data/orders.csv';

--patient_conditions
create or replace table raw.patient_conditions as select * from 'src_data/patient_conditions.csv';

-- visits
create or replace table raw.visits as select * from 'src_data/visits.csv';

-- conditions
create or replace table raw.conditions as select * from 'src_data/conditions.csv';

-- medications
create or replace table raw.medications as select * from 'src_data/medications.csv';

-- med_admins
create or replace table raw.med_admins as select * from 'src_data/med_admins.csv';

-- documents
create or replace table raw.documents as select * from 'src_data/documents.csv';
