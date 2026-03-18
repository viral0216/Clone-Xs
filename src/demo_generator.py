"""Demo Data Generator — creates realistic Unity Catalog demo catalogs with synthetic data.

Generates tables, views, and UDFs across 5 industry schemas using server-side
Databricks SQL (EXPLODE/SEQUENCE + random functions). No data is transferred
from the client.
"""

import logging
import time

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, execute_sql_parallel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Industry schema definitions
# ---------------------------------------------------------------------------

# Each industry has 20 tables, 20 views, 20 UDFs
# Row targets are at scale_factor=1.0 (~200M per industry, 1B total)

INDUSTRIES = {
    "healthcare": {
        "tables": [
            # Large fact tables
            {"name": "claims", "rows": 100_000_000, "ddl_cols": "claim_id BIGINT, patient_id BIGINT, provider_id BIGINT, diagnosis_code STRING, procedure_code STRING, claim_amount DECIMAL(10,2), submitted_date DATE, status STRING, payer_id BIGINT, facility_id BIGINT",
             "insert_expr": "id + {offset} AS claim_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*1000000)+1 AS provider_id, concat('ICD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS diagnosis_code, concat('CPT-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS procedure_code, round(rand()*50000+50,2) AS claim_amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS submitted_date, element_at(array('submitted','approved','denied','pending','appealed'),cast(floor(rand()*5)+1 as INT)) AS status, floor(rand()*100)+1 AS payer_id, floor(rand()*1000)+1 AS facility_id"},
            {"name": "encounters", "rows": 50_000_000, "ddl_cols": "encounter_id BIGINT, patient_id BIGINT, provider_id BIGINT, facility_id BIGINT, encounter_date DATE, encounter_type STRING, duration_minutes INT, discharge_status STRING, primary_diagnosis STRING, admission_source STRING",
             "insert_expr": "id + {offset} AS encounter_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*1000000)+1 AS provider_id, floor(rand()*1000)+1 AS facility_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS encounter_date, element_at(array('inpatient','outpatient','emergency','observation','telehealth'),cast(floor(rand()*5)+1 as INT)) AS encounter_type, cast(floor(rand()*480)+15 as INT) AS duration_minutes, element_at(array('discharged','transferred','deceased','left_ama','admitted'),cast(floor(rand()*5)+1 as INT)) AS discharge_status, concat('ICD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS primary_diagnosis, element_at(array('self','referral','transfer','emergency'),cast(floor(rand()*4)+1 as INT)) AS admission_source"},
            {"name": "prescriptions", "rows": 30_000_000, "ddl_cols": "rx_id BIGINT, patient_id BIGINT, provider_id BIGINT, drug_code STRING, drug_name STRING, quantity INT, days_supply INT, fill_date DATE, pharmacy_id BIGINT, refills_remaining INT",
             "insert_expr": "id + {offset} AS rx_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*1000000)+1 AS provider_id, concat('NDC-',lpad(cast(floor(rand()*999999) as STRING),6,'0')) AS drug_code, element_at(array('Lisinopril','Metformin','Atorvastatin','Amlodipine','Omeprazole','Metoprolol','Losartan','Albuterol','Gabapentin','Levothyroxine'),cast(floor(rand()*10)+1 as INT)) AS drug_name, cast(floor(rand()*90)+10 as INT) AS quantity, element_at(array(7,14,30,60,90),cast(floor(rand()*5)+1 as INT)) AS days_supply, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS fill_date, floor(rand()*5000)+1 AS pharmacy_id, cast(floor(rand()*6) as INT) AS refills_remaining"},
            # Medium tables
            {"name": "lab_results", "rows": 5_000_000, "ddl_cols": "result_id BIGINT, patient_id BIGINT, encounter_id BIGINT, test_code STRING, test_name STRING, result_value DOUBLE, unit STRING, reference_range STRING, collected_date DATE, status STRING",
             "insert_expr": "id + {offset} AS result_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*50000000)+1 AS encounter_id, concat('LAB-',lpad(cast(floor(rand()*9999) as STRING),4,'0')) AS test_code, element_at(array('CBC','BMP','CMP','Lipid Panel','TSH','HbA1c','Urinalysis','PT/INR','Troponin','D-Dimer'),cast(floor(rand()*10)+1 as INT)) AS test_name, round(rand()*500,2) AS result_value, element_at(array('mg/dL','mmol/L','g/dL','U/L','mEq/L','%','ng/mL'),cast(floor(rand()*7)+1 as INT)) AS unit, '0-100' AS reference_range, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS collected_date, element_at(array('final','preliminary','corrected'),cast(floor(rand()*3)+1 as INT)) AS status"},
            {"name": "vital_signs", "rows": 5_000_000, "ddl_cols": "vital_id BIGINT, patient_id BIGINT, encounter_id BIGINT, measurement_type STRING, value DOUBLE, unit STRING, recorded_at TIMESTAMP",
             "insert_expr": "id + {offset} AS vital_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*50000000)+1 AS encounter_id, element_at(array('blood_pressure','heart_rate','temperature','respiratory_rate','spo2','weight','height','bmi'),cast(floor(rand()*8)+1 as INT)) AS measurement_type, round(rand()*200+30,1) AS value, element_at(array('mmHg','bpm','°F','breaths/min','%','kg','cm','kg/m²'),cast(floor(rand()*8)+1 as INT)) AS unit, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS recorded_at"},
            # Dimension tables
            {"name": "patients", "rows": 1_000_000, "ddl_cols": "patient_id BIGINT, first_name STRING, last_name STRING, date_of_birth DATE, gender STRING, zip_code STRING, insurance_plan_id BIGINT, email STRING, phone STRING, created_at DATE",
             "insert_expr": "id + {offset} AS patient_id, element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, date_add('1940-01-01',cast(floor(rand()*29200) as INT)) AS date_of_birth, element_at(array('M','F','O'),cast(floor(rand()*3)+1 as INT)) AS gender, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip_code, floor(rand()*100)+1 AS insurance_plan_id, concat('patient',id,'@example.com') AS email, concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS created_at"},
            {"name": "providers", "rows": 1_000_000, "ddl_cols": "provider_id BIGINT, npi STRING, first_name STRING, last_name STRING, specialty_code STRING, facility_id BIGINT, license_state STRING, active BOOLEAN",
             "insert_expr": "id + {offset} AS provider_id, lpad(cast(floor(rand()*9999999999) as STRING),10,'0') AS npi, element_at(array('Sarah','David','Emily','James','Anna','Robert','Maria','William','Lisa','Thomas'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Chen','Patel','Kim','Singh','Lee','Wang','Kumar','Shah','Ali','Gupta'),cast(floor(rand()*10)+1 as INT)) AS last_name, element_at(array('CARD','DERM','ENDO','GAST','NEUR','ONCO','ORTH','PEDI','PSYC','SURG'),cast(floor(rand()*10)+1 as INT)) AS specialty_code, floor(rand()*1000)+1 AS facility_id, element_at(array('CA','NY','TX','FL','IL','PA','OH','GA','NC','MI'),cast(floor(rand()*10)+1 as INT)) AS license_state, rand() > 0.1 AS active"},
            {"name": "facilities", "rows": 1_000_000, "ddl_cols": "facility_id BIGINT, name STRING, facility_type STRING, address STRING, city STRING, state STRING, zip_code STRING, bed_count INT, npi STRING",
             "insert_expr": "id + {offset} AS facility_id, concat(element_at(array('Memorial','St. Mary','Regional','University','Community','General','Sacred Heart','Methodist','Baptist','Presbyterian'),cast(floor(rand()*10)+1 as INT)),' Hospital') AS name, element_at(array('hospital','clinic','urgent_care','nursing_facility','rehab','surgery_center'),cast(floor(rand()*6)+1 as INT)) AS facility_type, concat(cast(floor(rand()*9999)+1 as STRING),' Main St') AS address, element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','Austin'),cast(floor(rand()*10)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ','PA','TX','CA','TX','TX'),cast(floor(rand()*10)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip_code, cast(floor(rand()*500)+10 as INT) AS bed_count, lpad(cast(floor(rand()*9999999999) as STRING),10,'0') AS npi"},
            {"name": "insurance_plans", "rows": 1_000_000, "ddl_cols": "plan_id BIGINT, carrier_name STRING, plan_type STRING, coverage_level STRING, monthly_premium DECIMAL(8,2), deductible DECIMAL(8,2), effective_date DATE",
             "insert_expr": "id + {offset} AS plan_id, element_at(array('Aetna','Blue Cross','Cigna','UnitedHealth','Humana','Kaiser','Anthem','Molina','Centene','WellCare'),cast(floor(rand()*10)+1 as INT)) AS carrier_name, element_at(array('HMO','PPO','EPO','POS','HDHP'),cast(floor(rand()*5)+1 as INT)) AS plan_type, element_at(array('bronze','silver','gold','platinum'),cast(floor(rand()*4)+1 as INT)) AS coverage_level, round(rand()*800+100,2) AS monthly_premium, round(rand()*5000+500,2) AS deductible, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date"},
            {"name": "drug_catalog", "rows": 1_000_000, "ddl_cols": "drug_code STRING, drug_name STRING, generic_name STRING, drug_class STRING, manufacturer STRING, schedule STRING, unit_cost DECIMAL(8,2)",
             "insert_expr": "concat('NDC-',lpad(cast(id + {offset} as STRING),6,'0')) AS drug_code, element_at(array('Lisinopril','Metformin','Atorvastatin','Amlodipine','Omeprazole','Metoprolol','Losartan','Albuterol','Gabapentin','Levothyroxine'),cast(floor(rand()*10)+1 as INT)) AS drug_name, element_at(array('lisinopril','metformin','atorvastatin','amlodipine','omeprazole','metoprolol','losartan','albuterol','gabapentin','levothyroxine'),cast(floor(rand()*10)+1 as INT)) AS generic_name, element_at(array('ACE Inhibitor','Biguanide','Statin','CCB','PPI','Beta Blocker','ARB','Bronchodilator','Anticonvulsant','Thyroid'),cast(floor(rand()*10)+1 as INT)) AS drug_class, element_at(array('Pfizer','Merck','Johnson & Johnson','AbbVie','Roche','Novartis','Eli Lilly','Bristol-Myers','AstraZeneca','GSK'),cast(floor(rand()*10)+1 as INT)) AS manufacturer, element_at(array('II','III','IV','V','OTC'),cast(floor(rand()*5)+1 as INT)) AS schedule, round(rand()*500+1,2) AS unit_cost"},
            # Lookup tables
            {"name": "diagnosis_codes", "rows": 500_000, "ddl_cols": "code STRING, description STRING, category STRING, icd_version STRING, is_chronic BOOLEAN",
             "insert_expr": "concat('ICD-',lpad(cast(id + {offset} as STRING),5,'0')) AS code, concat('Diagnosis for code ',id + {offset}) AS description, element_at(array('Infectious','Neoplasms','Blood','Endocrine','Mental','Nervous','Circulatory','Respiratory','Digestive','Musculoskeletal'),cast(floor(rand()*10)+1 as INT)) AS category, element_at(array('ICD-10','ICD-11'),cast(floor(rand()*2)+1 as INT)) AS icd_version, rand() > 0.7 AS is_chronic"},
            {"name": "procedure_codes", "rows": 500_000, "ddl_cols": "code STRING, description STRING, category STRING, cpt_version STRING, avg_duration_minutes INT",
             "insert_expr": "concat('CPT-',lpad(cast(id + {offset} as STRING),5,'0')) AS code, concat('Procedure ',id + {offset}) AS description, element_at(array('Evaluation','Surgery','Radiology','Pathology','Medicine','Anesthesia'),cast(floor(rand()*6)+1 as INT)) AS category, '2024' AS cpt_version, cast(floor(rand()*240)+15 as INT) AS avg_duration_minutes"},
            {"name": "pharmacies", "rows": 200_000, "ddl_cols": "pharmacy_id BIGINT, name STRING, chain STRING, address STRING, city STRING, state STRING, is_24hr BOOLEAN",
             "insert_expr": "id + {offset} AS pharmacy_id, concat(element_at(array('CVS','Walgreens','Rite Aid','Walmart','Costco','Kroger','Publix','HEB','Safeway','Target'),cast(floor(rand()*10)+1 as INT)),' #',id) AS name, element_at(array('CVS','Walgreens','Rite Aid','Walmart','Independent'),cast(floor(rand()*5)+1 as INT)) AS chain, concat(cast(floor(rand()*9999)+1 as STRING),' Pharmacy Ave') AS address, element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ'),cast(floor(rand()*5)+1 as INT)) AS state, rand() > 0.8 AS is_24hr"},
            {"name": "specialties", "rows": 100_000, "ddl_cols": "specialty_code STRING, description STRING, department STRING, requires_referral BOOLEAN",
             "insert_expr": "concat('SPEC-',lpad(cast(id + {offset} as STRING),4,'0')) AS specialty_code, element_at(array('Cardiology','Dermatology','Endocrinology','Gastroenterology','Neurology','Oncology','Orthopedics','Pediatrics','Psychiatry','Surgery'),cast(floor(rand()*10)+1 as INT)) AS description, element_at(array('Medicine','Surgery','Diagnostics','Therapy','Research'),cast(floor(rand()*5)+1 as INT)) AS department, rand() > 0.5 AS requires_referral"},
            {"name": "claim_lines", "rows": 500_000, "ddl_cols": "line_id BIGINT, claim_id BIGINT, service_code STRING, quantity INT, line_amount DECIMAL(10,2), modifier STRING",
             "insert_expr": "id + {offset} AS line_id, floor(rand()*100000000)+1 AS claim_id, concat('SVC-',lpad(cast(floor(rand()*9999) as STRING),4,'0')) AS service_code, cast(floor(rand()*10)+1 as INT) AS quantity, round(rand()*5000+10,2) AS line_amount, element_at(array('26','TC','59','25','57','76','77',''),cast(floor(rand()*8)+1 as INT)) AS modifier"},
            {"name": "referrals", "rows": 300_000, "ddl_cols": "referral_id BIGINT, patient_id BIGINT, from_provider_id BIGINT, to_provider_id BIGINT, reason STRING, referral_date DATE, status STRING",
             "insert_expr": "id + {offset} AS referral_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*1000000)+1 AS from_provider_id, floor(rand()*1000000)+1 AS to_provider_id, element_at(array('specialist consult','second opinion','diagnostic imaging','surgical evaluation','therapy','follow-up'),cast(floor(rand()*6)+1 as INT)) AS reason, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS referral_date, element_at(array('pending','accepted','completed','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status"},
            {"name": "appointments", "rows": 500_000, "ddl_cols": "appt_id BIGINT, patient_id BIGINT, provider_id BIGINT, scheduled_date DATE, scheduled_time STRING, status STRING, appt_type STRING, duration_minutes INT",
             "insert_expr": "id + {offset} AS appt_id, floor(rand()*1000000)+1 AS patient_id, floor(rand()*1000000)+1 AS provider_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS scheduled_date, concat(lpad(cast(floor(rand()*12)+7 as STRING),2,'0'),':',element_at(array('00','15','30','45'),cast(floor(rand()*4)+1 as INT))) AS scheduled_time, element_at(array('scheduled','confirmed','completed','cancelled','no_show'),cast(floor(rand()*5)+1 as INT)) AS status, element_at(array('new_patient','follow_up','annual_physical','urgent','telehealth'),cast(floor(rand()*5)+1 as INT)) AS appt_type, element_at(array(15,30,45,60),cast(floor(rand()*4)+1 as INT)) AS duration_minutes"},
            {"name": "allergies", "rows": 200_000, "ddl_cols": "allergy_id BIGINT, patient_id BIGINT, allergen STRING, severity STRING, reaction STRING, onset_date DATE",
             "insert_expr": "id + {offset} AS allergy_id, floor(rand()*1000000)+1 AS patient_id, element_at(array('Penicillin','Sulfa','Latex','Peanuts','Shellfish','Aspirin','Ibuprofen','Codeine','Contrast Dye','Eggs'),cast(floor(rand()*10)+1 as INT)) AS allergen, element_at(array('mild','moderate','severe','life-threatening'),cast(floor(rand()*4)+1 as INT)) AS severity, element_at(array('rash','hives','anaphylaxis','swelling','nausea','breathing difficulty'),cast(floor(rand()*6)+1 as INT)) AS reaction, date_add('2010-01-01',cast(floor(rand()*5475) as INT)) AS onset_date"},
            {"name": "immunizations", "rows": 200_000, "ddl_cols": "imm_id BIGINT, patient_id BIGINT, vaccine_code STRING, vaccine_name STRING, administered_date DATE, lot_number STRING, site STRING",
             "insert_expr": "id + {offset} AS imm_id, floor(rand()*1000000)+1 AS patient_id, concat('CVX-',lpad(cast(floor(rand()*999) as STRING),3,'0')) AS vaccine_code, element_at(array('COVID-19','Influenza','Tdap','MMR','Hepatitis B','Pneumonia','Shingles','HPV','Meningitis','Varicella'),cast(floor(rand()*10)+1 as INT)) AS vaccine_name, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS administered_date, concat('LOT-',upper(substring(sha2(cast(id as STRING),256),1,8))) AS lot_number, element_at(array('left_arm','right_arm','left_thigh','right_thigh'),cast(floor(rand()*4)+1 as INT)) AS site"},
            {"name": "billing_adjustments", "rows": 500_000, "ddl_cols": "adj_id BIGINT, claim_id BIGINT, adjustment_type STRING, amount DECIMAL(10,2), reason_code STRING, adjusted_date DATE",
             "insert_expr": "id + {offset} AS adj_id, floor(rand()*100000000)+1 AS claim_id, element_at(array('contractual','write_off','payment','refund','correction'),cast(floor(rand()*5)+1 as INT)) AS adjustment_type, round(rand()*5000-2500,2) AS amount, element_at(array('CO-45','CO-97','PR-1','PR-2','OA-23','CO-4','PI-16'),cast(floor(rand()*7)+1 as INT)) AS reason_code, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS adjusted_date"},
        ],
        "views": [
            ("v_claim_summary", "SELECT p.patient_id, p.first_name, p.last_name, count(*) AS total_claims, sum(c.claim_amount) AS total_amount, avg(c.claim_amount) AS avg_amount, min(c.submitted_date) AS first_claim, max(c.submitted_date) AS last_claim FROM {s}.claims c JOIN {s}.patients p ON c.patient_id = p.patient_id GROUP BY p.patient_id, p.first_name, p.last_name"),
            ("v_provider_workload", "SELECT pr.provider_id, pr.first_name, pr.last_name, pr.specialty_code, count(DISTINCT e.patient_id) AS unique_patients, count(*) AS total_encounters, avg(e.duration_minutes) AS avg_duration FROM {s}.encounters e JOIN {s}.providers pr ON e.provider_id = pr.provider_id GROUP BY pr.provider_id, pr.first_name, pr.last_name, pr.specialty_code"),
            ("v_monthly_claims", "SELECT date_trunc('month', submitted_date) AS month, status, count(*) AS claim_count, sum(claim_amount) AS total_amount FROM {s}.claims GROUP BY date_trunc('month', submitted_date), status"),
            ("v_high_cost_patients", "SELECT patient_id, count(*) AS claim_count, sum(claim_amount) AS total_cost FROM {s}.claims GROUP BY patient_id HAVING sum(claim_amount) > 100000"),
            ("v_facility_utilization", "SELECT f.facility_id, f.name, f.bed_count, count(*) AS total_encounters, count(DISTINCT e.patient_id) AS unique_patients FROM {s}.facilities f JOIN {s}.encounters e ON f.facility_id = e.facility_id GROUP BY f.facility_id, f.name, f.bed_count"),
            ("v_prescription_trends", "SELECT date_trunc('month', fill_date) AS month, drug_name, count(*) AS rx_count, avg(quantity) AS avg_quantity FROM {s}.prescriptions GROUP BY date_trunc('month', fill_date), drug_name"),
            ("v_lab_abnormal_results", "SELECT lr.*, p.first_name, p.last_name FROM {s}.lab_results lr JOIN {s}.patients p ON lr.patient_id = p.patient_id WHERE lr.result_value > 200"),
            ("v_encounter_discharge", "SELECT encounter_type, discharge_status, count(*) AS cnt, avg(duration_minutes) AS avg_duration FROM {s}.encounters GROUP BY encounter_type, discharge_status"),
            ("v_drug_utilization", "SELECT dc.drug_class, dc.drug_name, count(*) AS prescriptions, sum(rx.quantity) AS total_units FROM {s}.prescriptions rx JOIN {s}.drug_catalog dc ON rx.drug_code = dc.drug_code GROUP BY dc.drug_class, dc.drug_name"),
            ("v_patient_demographics", "SELECT gender, floor(datediff(current_date(), date_of_birth)/365) AS age, count(*) AS patient_count FROM {s}.patients GROUP BY gender, floor(datediff(current_date(), date_of_birth)/365)"),
            ("v_active_referrals", "SELECT r.*, p.first_name, p.last_name FROM {s}.referrals r JOIN {s}.patients p ON r.patient_id = p.patient_id WHERE r.status = 'pending'"),
            ("v_appointment_no_shows", "SELECT provider_id, count(*) AS no_shows, count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY provider_id) AS no_show_pct FROM {s}.appointments WHERE status = 'no_show' GROUP BY provider_id"),
            ("v_allergy_prevalence", "SELECT allergen, severity, count(*) AS patient_count FROM {s}.allergies GROUP BY allergen, severity ORDER BY patient_count DESC"),
            ("v_immunization_coverage", "SELECT vaccine_name, date_trunc('quarter', administered_date) AS quarter, count(DISTINCT patient_id) AS patients_vaccinated FROM {s}.immunizations GROUP BY vaccine_name, date_trunc('quarter', administered_date)"),
            ("v_claim_denials", "SELECT c.diagnosis_code, c.procedure_code, count(*) AS denial_count, avg(c.claim_amount) AS avg_denied_amount FROM {s}.claims c WHERE c.status = 'denied' GROUP BY c.diagnosis_code, c.procedure_code"),
            ("v_pharmacy_volume", "SELECT ph.chain, ph.state, count(*) AS rx_filled FROM {s}.prescriptions rx JOIN {s}.pharmacies ph ON rx.pharmacy_id = ph.pharmacy_id GROUP BY ph.chain, ph.state"),
            ("v_billing_net", "SELECT cl.claim_id, cl.claim_amount, COALESCE(sum(ba.amount),0) AS total_adjustments, cl.claim_amount + COALESCE(sum(ba.amount),0) AS net_amount FROM {s}.claims cl LEFT JOIN {s}.billing_adjustments ba ON cl.claim_id = ba.claim_id GROUP BY cl.claim_id, cl.claim_amount"),
            ("v_specialty_demand", "SELECT sp.description AS specialty, count(*) AS referral_count FROM {s}.referrals r JOIN {s}.providers p ON r.to_provider_id = p.provider_id JOIN {s}.specialties sp ON p.specialty_code = sp.specialty_code GROUP BY sp.description"),
            ("v_vital_signs_latest", "SELECT patient_id, measurement_type, value, unit, recorded_at, ROW_NUMBER() OVER (PARTITION BY patient_id, measurement_type ORDER BY recorded_at DESC) AS rn FROM {s}.vital_signs"),
            ("v_patient_360", "SELECT p.patient_id, p.first_name, p.last_name, p.gender, count(DISTINCT c.claim_id) AS claims, count(DISTINCT e.encounter_id) AS encounters, count(DISTINCT rx.rx_id) AS prescriptions FROM {s}.patients p LEFT JOIN {s}.claims c ON p.patient_id = c.patient_id LEFT JOIN {s}.encounters e ON p.patient_id = e.patient_id LEFT JOIN {s}.prescriptions rx ON p.patient_id = rx.patient_id GROUP BY p.patient_id, p.first_name, p.last_name, p.gender"),
        ],
        "udfs": [
            ("mask_patient_id", "id BIGINT", "STRING", "Masks patient ID for de-identification", "concat('PAT-',substring(sha2(cast(id AS STRING),256),1,12))"),
            ("mask_ssn", "ssn STRING", "STRING", "Masks SSN showing only last 4 digits", "concat('***-**-',right(ssn,4))"),
            ("mask_email", "email STRING", "STRING", "Masks email address", "concat(left(email,2),'***@',split(email,'@')[1])"),
            ("mask_phone", "phone STRING", "STRING", "Masks phone number", "concat('***-***-',right(phone,4))"),
            ("calculate_age", "dob DATE", "INT", "Calculates age from date of birth", "floor(datediff(current_date(),dob)/365.25)"),
            ("format_claim_amount", "amount DECIMAL(10,2)", "STRING", "Formats claim amount as currency", "concat('$',format_number(amount,2))"),
            ("is_valid_npi", "npi STRING", "BOOLEAN", "Validates NPI format (10 digits)", "length(npi) = 10 AND npi RLIKE '^[0-9]+$'"),
            ("risk_score_band", "score DOUBLE", "STRING", "Categorizes risk score into bands", "CASE WHEN score >= 80 THEN 'Critical' WHEN score >= 60 THEN 'High' WHEN score >= 40 THEN 'Medium' WHEN score >= 20 THEN 'Low' ELSE 'Minimal' END"),
            ("icd_category", "code STRING", "STRING", "Extracts ICD category from code", "substring(code,5,2)"),
            ("days_since", "dt DATE", "INT", "Days since a given date", "datediff(current_date(),dt)"),
            ("is_chronic", "diagnosis STRING", "BOOLEAN", "Checks if diagnosis code indicates chronic condition", "diagnosis RLIKE '^ICD-(E|I|J4|M|N1)'"),
            ("encounter_duration_category", "minutes INT", "STRING", "Categorizes encounter duration", "CASE WHEN minutes < 30 THEN 'Brief' WHEN minutes < 60 THEN 'Standard' WHEN minutes < 120 THEN 'Extended' ELSE 'Long' END"),
            ("format_date_display", "dt DATE", "STRING", "Formats date for display", "date_format(dt,'MMM dd, yyyy')"),
            ("quarter_label", "dt DATE", "STRING", "Returns quarter label like Q1 2024", "concat('Q',quarter(dt),' ',year(dt))"),
            ("bmi_category", "bmi DOUBLE", "STRING", "BMI classification", "CASE WHEN bmi < 18.5 THEN 'Underweight' WHEN bmi < 25 THEN 'Normal' WHEN bmi < 30 THEN 'Overweight' ELSE 'Obese' END"),
            ("claim_status_label", "status STRING", "STRING", "Human-readable claim status", "initcap(replace(status,'_',' '))"),
            ("drug_schedule_risk", "schedule STRING", "STRING", "Risk level from drug schedule", "CASE WHEN schedule = 'II' THEN 'High' WHEN schedule IN ('III','IV') THEN 'Medium' WHEN schedule = 'V' THEN 'Low' ELSE 'None' END"),
            ("is_pediatric", "dob DATE", "BOOLEAN", "Checks if patient is under 18", "datediff(current_date(),dob) < 6570"),
            ("insurance_tier", "coverage STRING", "INT", "Numeric tier from coverage level", "CASE WHEN coverage = 'platinum' THEN 4 WHEN coverage = 'gold' THEN 3 WHEN coverage = 'silver' THEN 2 ELSE 1 END"),
            ("anonymize_name", "first_name STRING, last_name STRING", "STRING", "Creates anonymized identifier", "concat(left(first_name,1),'.',left(last_name,1),'.',substring(sha2(concat(first_name,last_name),256),1,6))"),
        ],
    },
}

# Define the other 4 industries with the same structure
INDUSTRIES["financial"] = {
    "tables": [
        {"name": "transactions", "rows": 100_000_000, "ddl_cols": "txn_id BIGINT, account_id BIGINT, txn_type STRING, amount DECIMAL(12,2), currency STRING, txn_date DATE, merchant_id BIGINT, category STRING, channel STRING, status STRING",
         "insert_expr": "id + {offset} AS txn_id, floor(rand()*1000000)+1 AS account_id, element_at(array('debit','credit','transfer','payment','withdrawal','deposit'),cast(floor(rand()*6)+1 as INT)) AS txn_type, round(rand()*10000-500,2) AS amount, element_at(array('USD','EUR','GBP','CAD','AUD'),cast(floor(rand()*5)+1 as INT)) AS currency, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS txn_date, floor(rand()*500000)+1 AS merchant_id, element_at(array('groceries','dining','travel','utilities','entertainment','healthcare','shopping','fuel','insurance','education'),cast(floor(rand()*10)+1 as INT)) AS category, element_at(array('online','in_store','mobile','atm','branch'),cast(floor(rand()*5)+1 as INT)) AS channel, element_at(array('completed','pending','reversed','declined'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "card_events", "rows": 50_000_000, "ddl_cols": "event_id BIGINT, card_id BIGINT, event_type STRING, amount DECIMAL(12,2), merchant_name STRING, mcc_code STRING, event_timestamp TIMESTAMP, country STRING, is_international BOOLEAN, auth_code STRING",
         "insert_expr": "id + {offset} AS event_id, floor(rand()*2000000)+1 AS card_id, element_at(array('authorization','capture','refund','chargeback','reversal'),cast(floor(rand()*5)+1 as INT)) AS event_type, round(rand()*5000+1,2) AS amount, concat(element_at(array('Amazon','Walmart','Target','Starbucks','Shell','Uber','Netflix','Apple','Google','Microsoft'),cast(floor(rand()*10)+1 as INT)),' #',floor(rand()*999)+1) AS merchant_name, lpad(cast(floor(rand()*9999) as STRING),4,'0') AS mcc_code, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp, element_at(array('US','UK','CA','DE','FR','JP','AU','BR','IN','MX'),cast(floor(rand()*10)+1 as INT)) AS country, rand() > 0.85 AS is_international, upper(substring(sha2(cast(id as STRING),256),1,6)) AS auth_code"},
        {"name": "loan_payments", "rows": 30_000_000, "ddl_cols": "payment_id BIGINT, loan_id BIGINT, payment_date DATE, principal_amount DECIMAL(12,2), interest_amount DECIMAL(12,2), total_amount DECIMAL(12,2), remaining_balance DECIMAL(12,2), payment_method STRING, status STRING, days_late INT",
         "insert_expr": "id + {offset} AS payment_id, floor(rand()*5000000)+1 AS loan_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS payment_date, round(rand()*2000+100,2) AS principal_amount, round(rand()*500+10,2) AS interest_amount, round(rand()*2500+110,2) AS total_amount, round(rand()*500000,2) AS remaining_balance, element_at(array('autopay','online','check','wire','branch'),cast(floor(rand()*5)+1 as INT)) AS payment_method, element_at(array('on_time','late','partial','missed'),cast(floor(rand()*4)+1 as INT)) AS status, CASE WHEN rand() > 0.8 THEN cast(floor(rand()*90) as INT) ELSE 0 END AS days_late"},
        {"name": "wire_transfers", "rows": 5_000_000, "ddl_cols": "wire_id BIGINT, sender_account BIGINT, receiver_account BIGINT, amount DECIMAL(14,2), currency STRING, wire_date DATE, swift_code STRING, status STRING, purpose STRING, fee DECIMAL(8,2)",
         "insert_expr": "id + {offset} AS wire_id, floor(rand()*1000000)+1 AS sender_account, floor(rand()*1000000)+1 AS receiver_account, round(rand()*100000+100,2) AS amount, element_at(array('USD','EUR','GBP','CHF','JPY'),cast(floor(rand()*5)+1 as INT)) AS currency, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS wire_date, upper(substring(sha2(cast(id as STRING),256),1,8)) AS swift_code, element_at(array('completed','pending','cancelled','returned'),cast(floor(rand()*4)+1 as INT)) AS status, element_at(array('invoice','payroll','investment','loan','personal'),cast(floor(rand()*5)+1 as INT)) AS purpose, round(rand()*50+5,2) AS fee"},
        {"name": "trading_orders", "rows": 5_000_000, "ddl_cols": "order_id BIGINT, account_id BIGINT, symbol STRING, side STRING, quantity INT, price DECIMAL(12,4), order_type STRING, order_date DATE, filled_at TIMESTAMP, status STRING",
         "insert_expr": "id + {offset} AS order_id, floor(rand()*1000000)+1 AS account_id, element_at(array('AAPL','MSFT','GOOGL','AMZN','META','TSLA','NVDA','JPM','V','JNJ'),cast(floor(rand()*10)+1 as INT)) AS symbol, element_at(array('buy','sell'),cast(floor(rand()*2)+1 as INT)) AS side, cast(floor(rand()*1000)+1 as INT) AS quantity, round(rand()*500+10,4) AS price, element_at(array('market','limit','stop','stop_limit'),cast(floor(rand()*4)+1 as INT)) AS order_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS order_date, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS filled_at, element_at(array('filled','partial','cancelled','pending'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "accounts", "rows": 1_000_000, "ddl_cols": "account_id BIGINT, customer_id BIGINT, account_type STRING, balance DECIMAL(14,2), currency STRING, opened_date DATE, status STRING, branch_id BIGINT, interest_rate DECIMAL(5,4), overdraft_limit DECIMAL(10,2)",
         "insert_expr": "id + {offset} AS account_id, floor(rand()*500000)+1 AS customer_id, element_at(array('checking','savings','money_market','cd','ira','brokerage'),cast(floor(rand()*6)+1 as INT)) AS account_type, round(rand()*500000-1000,2) AS balance, 'USD' AS currency, date_add('2010-01-01',cast(floor(rand()*5475) as INT)) AS opened_date, element_at(array('active','dormant','closed','frozen'),cast(floor(rand()*4)+1 as INT)) AS status, floor(rand()*200)+1 AS branch_id, round(rand()*0.05,4) AS interest_rate, round(rand()*5000,2) AS overdraft_limit"},
        {"name": "customers", "rows": 1_000_000, "ddl_cols": "customer_id BIGINT, first_name STRING, last_name STRING, email STRING, phone STRING, ssn_hash STRING, date_of_birth DATE, credit_score INT, income_bracket STRING, risk_rating STRING",
         "insert_expr": "id + {offset} AS customer_id, element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, concat('customer',id + {offset},'@example.com') AS email, concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone, sha2(cast(id as STRING),256) AS ssn_hash, date_add('1950-01-01',cast(floor(rand()*21900) as INT)) AS date_of_birth, cast(floor(rand()*450)+350 as INT) AS credit_score, element_at(array('0-25k','25k-50k','50k-75k','75k-100k','100k-150k','150k+'),cast(floor(rand()*6)+1 as INT)) AS income_bracket, element_at(array('low','medium','high','very_high'),cast(floor(rand()*4)+1 as INT)) AS risk_rating"},
        {"name": "branches", "rows": 1_000_000, "ddl_cols": "branch_id BIGINT, name STRING, address STRING, city STRING, state STRING, zip STRING, manager_id BIGINT, open_date DATE, branch_type STRING",
         "insert_expr": "id + {offset} AS branch_id, concat(element_at(array('Downtown','Midtown','Uptown','Westside','Eastside','North','South','Central','Harbor','Park'),cast(floor(rand()*10)+1 as INT)),' Branch') AS name, concat(cast(floor(rand()*9999)+1 as STRING),' Bank St') AS address, element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','Austin'),cast(floor(rand()*10)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ','PA','TX','CA','TX','TX'),cast(floor(rand()*10)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip, floor(rand()*10000)+1 AS manager_id, date_add('1990-01-01',cast(floor(rand()*12775) as INT)) AS open_date, element_at(array('full_service','express','digital','private'),cast(floor(rand()*4)+1 as INT)) AS branch_type"},
        {"name": "cards", "rows": 1_000_000, "ddl_cols": "card_id BIGINT, account_id BIGINT, card_type STRING, card_network STRING, card_number_hash STRING, expiry_date DATE, credit_limit DECIMAL(10,2), issued_date DATE, status STRING",
         "insert_expr": "id + {offset} AS card_id, floor(rand()*1000000)+1 AS account_id, element_at(array('credit','debit','prepaid','corporate'),cast(floor(rand()*4)+1 as INT)) AS card_type, element_at(array('Visa','Mastercard','Amex','Discover'),cast(floor(rand()*4)+1 as INT)) AS card_network, sha2(cast(id as STRING),256) AS card_number_hash, date_add(current_date(),cast(floor(rand()*1095) as INT)) AS expiry_date, round(rand()*50000+500,2) AS credit_limit, date_add('2018-01-01',cast(floor(rand()*2555) as INT)) AS issued_date, element_at(array('active','blocked','expired','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "loans", "rows": 1_000_000, "ddl_cols": "loan_id BIGINT, customer_id BIGINT, loan_type STRING, principal DECIMAL(14,2), interest_rate DECIMAL(5,4), term_months INT, start_date DATE, status STRING, collateral_type STRING",
         "insert_expr": "id + {offset} AS loan_id, floor(rand()*1000000)+1 AS customer_id, element_at(array('mortgage','auto','personal','student','business','home_equity'),cast(floor(rand()*6)+1 as INT)) AS loan_type, round(rand()*500000+1000,2) AS principal, round(rand()*0.15+0.02,4) AS interest_rate, element_at(array(12,24,36,48,60,120,180,240,360),cast(floor(rand()*9)+1 as INT)) AS term_months, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS start_date, element_at(array('active','paid_off','default','delinquent','restructured'),cast(floor(rand()*5)+1 as INT)) AS status, element_at(array('real_estate','vehicle','none','securities','equipment'),cast(floor(rand()*5)+1 as INT)) AS collateral_type"},
        {"name": "fraud_alerts", "rows": 500_000, "ddl_cols": "alert_id BIGINT, account_id BIGINT, txn_id BIGINT, alert_type STRING, risk_score DOUBLE, alert_date DATE, resolution STRING, investigated_by STRING",
         "insert_expr": "id + {offset} AS alert_id, floor(rand()*1000000)+1 AS account_id, floor(rand()*100000000)+1 AS txn_id, element_at(array('unusual_amount','foreign_country','velocity','device_change','ip_mismatch','account_takeover'),cast(floor(rand()*6)+1 as INT)) AS alert_type, round(rand()*100,1) AS risk_score, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS alert_date, element_at(array('confirmed_fraud','false_positive','under_review','escalated'),cast(floor(rand()*4)+1 as INT)) AS resolution, concat('analyst_',floor(rand()*100)+1) AS investigated_by"},
        {"name": "merchants", "rows": 500_000, "ddl_cols": "merchant_id BIGINT, name STRING, mcc_code STRING, category STRING, city STRING, state STRING, country STRING, risk_level STRING",
         "insert_expr": "id + {offset} AS merchant_id, concat(element_at(array('Quick','Prime','Star','Gold','Metro','Elite','First','Crown','Royal','Grand'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Mart','Shop','Store','Plus','Express','Market','Hub','Center','Zone','Place'),cast(floor(rand()*10)+1 as INT))) AS name, lpad(cast(floor(rand()*9999) as STRING),4,'0') AS mcc_code, element_at(array('retail','dining','travel','services','entertainment','groceries'),cast(floor(rand()*6)+1 as INT)) AS category, element_at(array('New York','LA','Chicago','Houston','Phoenix'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ'),cast(floor(rand()*5)+1 as INT)) AS state, 'US' AS country, element_at(array('low','medium','high'),cast(floor(rand()*3)+1 as INT)) AS risk_level"},
        {"name": "interest_rates", "rows": 200_000, "ddl_cols": "rate_id BIGINT, product_type STRING, term STRING, rate DECIMAL(5,4), effective_date DATE, published_by STRING",
         "insert_expr": "id + {offset} AS rate_id, element_at(array('savings','cd_6m','cd_12m','cd_24m','mortgage_15','mortgage_30','auto','personal','heloc'),cast(floor(rand()*9)+1 as INT)) AS product_type, element_at(array('6m','12m','24m','36m','60m','180m','360m'),cast(floor(rand()*7)+1 as INT)) AS term, round(rand()*0.08+0.01,4) AS rate, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date, 'Federal Reserve' AS published_by"},
        {"name": "compliance_events", "rows": 300_000, "ddl_cols": "event_id BIGINT, account_id BIGINT, event_type STRING, description STRING, reported_date DATE, resolution_date DATE, status STRING",
         "insert_expr": "id + {offset} AS event_id, floor(rand()*1000000)+1 AS account_id, element_at(array('SAR','CTR','KYC_update','AML_alert','OFAC_hit','PEP_match'),cast(floor(rand()*6)+1 as INT)) AS event_type, concat('Compliance event for account ',floor(rand()*1000000)+1) AS description, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS reported_date, date_add('2020-01-01',cast(floor(rand()*1825)+30 as INT)) AS resolution_date, element_at(array('open','resolved','escalated','dismissed'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "credit_scores", "rows": 300_000, "ddl_cols": "score_id BIGINT, customer_id BIGINT, score INT, score_date DATE, bureau STRING, factors STRING",
         "insert_expr": "id + {offset} AS score_id, floor(rand()*1000000)+1 AS customer_id, cast(floor(rand()*450)+350 as INT) AS score, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS score_date, element_at(array('Experian','TransUnion','Equifax'),cast(floor(rand()*3)+1 as INT)) AS bureau, element_at(array('payment_history','credit_utilization','length_of_credit','new_credit','credit_mix'),cast(floor(rand()*5)+1 as INT)) AS factors"},
        {"name": "atm_transactions", "rows": 500_000, "ddl_cols": "atm_txn_id BIGINT, card_id BIGINT, atm_id STRING, txn_type STRING, amount DECIMAL(10,2), txn_date DATE, surcharge DECIMAL(6,2), success BOOLEAN",
         "insert_expr": "id + {offset} AS atm_txn_id, floor(rand()*2000000)+1 AS card_id, concat('ATM-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS atm_id, element_at(array('withdrawal','balance_inquiry','deposit','transfer'),cast(floor(rand()*4)+1 as INT)) AS txn_type, round(rand()*1000+20,2) AS amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS txn_date, CASE WHEN rand() > 0.7 THEN round(rand()*5+1,2) ELSE 0 END AS surcharge, rand() > 0.05 AS success"},
        {"name": "account_statements", "rows": 200_000, "ddl_cols": "statement_id BIGINT, account_id BIGINT, statement_date DATE, opening_balance DECIMAL(14,2), closing_balance DECIMAL(14,2), total_debits DECIMAL(14,2), total_credits DECIMAL(14,2)",
         "insert_expr": "id + {offset} AS statement_id, floor(rand()*1000000)+1 AS account_id, date_add('2020-01-01',cast(floor(rand()*60)*30 as INT)) AS statement_date, round(rand()*100000,2) AS opening_balance, round(rand()*100000,2) AS closing_balance, round(rand()*50000,2) AS total_debits, round(rand()*50000,2) AS total_credits"},
        {"name": "beneficiaries", "rows": 200_000, "ddl_cols": "beneficiary_id BIGINT, account_id BIGINT, name STRING, relationship STRING, bank_name STRING, account_number_hash STRING, added_date DATE",
         "insert_expr": "id + {offset} AS beneficiary_id, floor(rand()*1000000)+1 AS account_id, concat(element_at(array('James','Mary','John','Patricia','Robert'),cast(floor(rand()*5)+1 as INT)),' ',element_at(array('Smith','Johnson','Williams','Brown','Jones'),cast(floor(rand()*5)+1 as INT))) AS name, element_at(array('spouse','child','parent','sibling','other'),cast(floor(rand()*5)+1 as INT)) AS relationship, element_at(array('Chase','BofA','Wells Fargo','Citi','US Bank'),cast(floor(rand()*5)+1 as INT)) AS bank_name, sha2(cast(id as STRING),256) AS account_number_hash, date_add('2018-01-01',cast(floor(rand()*2555) as INT)) AS added_date"},
        {"name": "exchange_rates", "rows": 100_000, "ddl_cols": "rate_id BIGINT, from_currency STRING, to_currency STRING, rate DECIMAL(10,6), rate_date DATE, source STRING",
         "insert_expr": "id + {offset} AS rate_id, element_at(array('USD','EUR','GBP','CHF','JPY','CAD','AUD'),cast(floor(rand()*7)+1 as INT)) AS from_currency, element_at(array('USD','EUR','GBP','CHF','JPY','CAD','AUD'),cast(floor(rand()*7)+1 as INT)) AS to_currency, round(rand()*2+0.5,6) AS rate, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS rate_date, element_at(array('ECB','Fed','Bloomberg','Reuters'),cast(floor(rand()*4)+1 as INT)) AS source"},
        ],
        "views": [
            ("v_account_summary", "SELECT a.account_id, a.account_type, a.balance, c.first_name, c.last_name, c.credit_score FROM {s}.accounts a JOIN {s}.customers c ON a.customer_id = c.customer_id"),
            ("v_monthly_txn_volume", "SELECT date_trunc('month',txn_date) AS month, txn_type, count(*) AS txn_count, sum(amount) AS total_amount FROM {s}.transactions GROUP BY date_trunc('month',txn_date), txn_type"),
            ("v_fraud_risk_summary", "SELECT f.alert_type, f.resolution, count(*) AS alert_count, avg(f.risk_score) AS avg_score FROM {s}.fraud_alerts f GROUP BY f.alert_type, f.resolution"),
            ("v_loan_portfolio", "SELECT l.loan_type, l.status, count(*) AS loan_count, sum(l.principal) AS total_principal, avg(l.interest_rate) AS avg_rate FROM {s}.loans l GROUP BY l.loan_type, l.status"),
            ("v_card_spending", "SELECT ce.country, ce.is_international, count(*) AS events, sum(ce.amount) AS total_spend FROM {s}.card_events ce GROUP BY ce.country, ce.is_international"),
            ("v_high_value_customers", "SELECT c.customer_id, c.first_name, c.last_name, sum(a.balance) AS total_balance FROM {s}.customers c JOIN {s}.accounts a ON c.customer_id = a.customer_id GROUP BY c.customer_id, c.first_name, c.last_name HAVING sum(a.balance) > 100000"),
            ("v_delinquent_loans", "SELECT l.*, c.first_name, c.last_name, c.credit_score FROM {s}.loans l JOIN {s}.customers c ON l.customer_id = c.customer_id WHERE l.status IN ('default','delinquent')"),
            ("v_branch_performance", "SELECT b.name, b.city, b.state, count(DISTINCT a.account_id) AS accounts, sum(a.balance) AS total_deposits FROM {s}.branches b JOIN {s}.accounts a ON b.branch_id = a.branch_id GROUP BY b.name, b.city, b.state"),
            ("v_payment_delinquency", "SELECT loan_id, count(*) AS late_payments, avg(days_late) AS avg_days_late, max(days_late) AS max_days_late FROM {s}.loan_payments WHERE days_late > 0 GROUP BY loan_id"),
            ("v_merchant_risk", "SELECT m.name, m.category, m.risk_level, count(f.alert_id) AS fraud_alerts FROM {s}.merchants m LEFT JOIN {s}.fraud_alerts f ON m.merchant_id = f.account_id GROUP BY m.name, m.category, m.risk_level"),
            ("v_compliance_dashboard", "SELECT event_type, status, count(*) AS event_count, avg(datediff(resolution_date,reported_date)) AS avg_resolution_days FROM {s}.compliance_events GROUP BY event_type, status"),
            ("v_credit_score_trend", "SELECT cs.customer_id, cs.bureau, cs.score, cs.score_date, LAG(cs.score) OVER (PARTITION BY cs.customer_id, cs.bureau ORDER BY cs.score_date) AS prev_score FROM {s}.credit_scores cs"),
            ("v_wire_transfer_volume", "SELECT date_trunc('month',wire_date) AS month, currency, count(*) AS transfers, sum(amount) AS total_amount FROM {s}.wire_transfers GROUP BY date_trunc('month',wire_date), currency"),
            ("v_atm_usage", "SELECT atm_id, count(*) AS txn_count, sum(amount) AS total_withdrawn, avg(surcharge) AS avg_surcharge FROM {s}.atm_transactions GROUP BY atm_id"),
            ("v_trading_performance", "SELECT symbol, side, count(*) AS orders, sum(quantity * price) AS total_value, avg(price) AS avg_price FROM {s}.trading_orders WHERE status = 'filled' GROUP BY symbol, side"),
            ("v_customer_360", "SELECT c.customer_id, c.first_name, c.last_name, c.credit_score, count(DISTINCT a.account_id) AS accounts, count(DISTINCT l.loan_id) AS loans, count(DISTINCT ca.card_id) AS cards FROM {s}.customers c LEFT JOIN {s}.accounts a ON c.customer_id = a.customer_id LEFT JOIN {s}.loans l ON c.customer_id = l.customer_id LEFT JOIN {s}.cards ca ON a.account_id = ca.account_id GROUP BY c.customer_id, c.first_name, c.last_name, c.credit_score"),
            ("v_daily_balance", "SELECT account_id, statement_date, closing_balance, closing_balance - opening_balance AS net_change FROM {s}.account_statements"),
            ("v_interest_rate_history", "SELECT product_type, effective_date, rate, LAG(rate) OVER (PARTITION BY product_type ORDER BY effective_date) AS prev_rate FROM {s}.interest_rates"),
            ("v_beneficiary_network", "SELECT b.account_id, b.name, b.bank_name, a.account_type, a.balance FROM {s}.beneficiaries b JOIN {s}.accounts a ON b.account_id = a.account_id"),
            ("v_exchange_rate_spread", "SELECT from_currency, to_currency, rate_date, rate, avg(rate) OVER (PARTITION BY from_currency, to_currency ORDER BY rate_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg FROM {s}.exchange_rates"),
        ],
        "udfs": [
            ("mask_account_number", "acct STRING", "STRING", "Masks account number", "concat('****',right(acct,4))"),
            ("mask_card_number", "card STRING", "STRING", "Masks card number", "concat('****-****-****-',right(card,4))"),
            ("mask_ssn_full", "ssn STRING", "STRING", "Fully masks SSN", "'***-**-****'"),
            ("format_currency", "amount DECIMAL(14,2), currency STRING", "STRING", "Formats amount with currency symbol", "CASE WHEN currency = 'USD' THEN concat('$',format_number(amount,2)) WHEN currency = 'EUR' THEN concat('€',format_number(amount,2)) WHEN currency = 'GBP' THEN concat('£',format_number(amount,2)) ELSE concat(currency,' ',format_number(amount,2)) END"),
            ("credit_score_band", "score INT", "STRING", "Categorizes credit score", "CASE WHEN score >= 800 THEN 'Exceptional' WHEN score >= 740 THEN 'Very Good' WHEN score >= 670 THEN 'Good' WHEN score >= 580 THEN 'Fair' ELSE 'Poor' END"),
            ("calculate_apr", "rate DECIMAL(5,4), compounds_per_year INT", "DECIMAL(5,4)", "Converts nominal rate to APR", "round(power(1 + rate/compounds_per_year, compounds_per_year) - 1, 4)"),
            ("is_high_risk_txn", "amount DECIMAL(12,2), is_international BOOLEAN", "BOOLEAN", "Flags high-risk transactions", "amount > 5000 OR is_international"),
            ("txn_category_label", "cat STRING", "STRING", "Human-readable transaction category", "initcap(replace(cat,'_',' '))"),
            ("loan_status_severity", "status STRING", "INT", "Numeric severity for loan status", "CASE WHEN status = 'default' THEN 5 WHEN status = 'delinquent' THEN 4 WHEN status = 'restructured' THEN 3 WHEN status = 'active' THEN 2 ELSE 1 END"),
            ("days_to_maturity", "start_date DATE, term_months INT", "INT", "Calculates days until loan maturity", "datediff(add_months(start_date, term_months), current_date())"),
            ("is_valid_swift", "code STRING", "BOOLEAN", "Validates SWIFT code format", "length(code) IN (8,11) AND code RLIKE '^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$'"),
            ("risk_weighted_amount", "amount DECIMAL(14,2), risk_level STRING", "DECIMAL(14,2)", "Risk-weighted asset calculation", "amount * CASE WHEN risk_level = 'high' THEN 1.5 WHEN risk_level = 'medium' THEN 1.0 ELSE 0.5 END"),
            ("format_iban", "iban STRING", "STRING", "Formats IBAN with spaces", "concat(substring(iban,1,4),' ',substring(iban,5,4),' ',substring(iban,9,4),' ',substring(iban,13))"),
            ("card_expiry_status", "expiry DATE", "STRING", "Checks card expiry status", "CASE WHEN expiry < current_date() THEN 'Expired' WHEN expiry < date_add(current_date(),90) THEN 'Expiring Soon' ELSE 'Valid' END"),
            ("monthly_payment", "principal DECIMAL(14,2), annual_rate DECIMAL(5,4), term_months INT", "DECIMAL(10,2)", "Calculates monthly loan payment", "round(principal * (annual_rate/12) / (1 - power(1 + annual_rate/12, -term_months)), 2)"),
            ("fraud_risk_label", "score DOUBLE", "STRING", "Labels fraud risk score", "CASE WHEN score >= 80 THEN 'Critical' WHEN score >= 60 THEN 'High' WHEN score >= 40 THEN 'Medium' WHEN score >= 20 THEN 'Low' ELSE 'Minimal' END"),
            ("anonymize_customer", "first_name STRING, last_name STRING, customer_id BIGINT", "STRING", "Creates anonymized customer ref", "concat('CUST-',substring(sha2(concat(first_name,last_name,cast(customer_id as STRING)),256),1,10))"),
            ("is_business_hours", "ts TIMESTAMP", "BOOLEAN", "Checks if timestamp is during business hours", "hour(ts) BETWEEN 9 AND 17 AND dayofweek(ts) BETWEEN 2 AND 6"),
            ("currency_convert", "amount DECIMAL(14,2), rate DECIMAL(10,6)", "DECIMAL(14,2)", "Converts amount using exchange rate", "round(amount * rate, 2)"),
            ("compliance_priority", "event_type STRING", "INT", "Priority level for compliance events", "CASE WHEN event_type IN ('SAR','OFAC_hit') THEN 1 WHEN event_type IN ('AML_alert','PEP_match') THEN 2 WHEN event_type = 'CTR' THEN 3 ELSE 4 END"),
        ],
    }

# Retail, Telecom, Manufacturing follow the same pattern — abbreviated for file size
INDUSTRIES["retail"] = {
    "tables": [
        {"name": "order_items", "rows": 100_000_000, "ddl_cols": "item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DECIMAL(10,2), discount DECIMAL(5,2), total DECIMAL(10,2), shipped_date DATE, status STRING, warehouse_id BIGINT",
         "insert_expr": "id + {offset} AS item_id, floor(rand()*30000000)+1 AS order_id, floor(rand()*500000)+1 AS product_id, cast(floor(rand()*10)+1 as INT) AS quantity, round(rand()*500+1,2) AS unit_price, round(rand()*0.3,2) AS discount, round((rand()*500+1)*(floor(rand()*10)+1)*(1-rand()*0.3),2) AS total, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS shipped_date, element_at(array('delivered','shipped','processing','returned','cancelled'),cast(floor(rand()*5)+1 as INT)) AS status, floor(rand()*100)+1 AS warehouse_id"},
        {"name": "clickstream", "rows": 50_000_000, "ddl_cols": "event_id BIGINT, session_id STRING, customer_id BIGINT, page_url STRING, event_type STRING, product_id BIGINT, event_timestamp TIMESTAMP, device_type STRING, referrer STRING, duration_seconds INT",
         "insert_expr": "id + {offset} AS event_id, concat('sess-',substring(sha2(cast(floor(id/20) as STRING),256),1,12)) AS session_id, floor(rand()*2000000)+1 AS customer_id, element_at(array('/home','/products','/product/detail','/cart','/checkout','/account','/search','/category','/deals','/help'),cast(floor(rand()*10)+1 as INT)) AS page_url, element_at(array('page_view','click','add_to_cart','remove_from_cart','search','purchase','scroll','hover'),cast(floor(rand()*8)+1 as INT)) AS event_type, floor(rand()*500000)+1 AS product_id, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp, element_at(array('desktop','mobile','tablet','app'),cast(floor(rand()*4)+1 as INT)) AS device_type, element_at(array('google','facebook','email','direct','instagram','tiktok'),cast(floor(rand()*6)+1 as INT)) AS referrer, cast(floor(rand()*300)+1 as INT) AS duration_seconds"},
        {"name": "reviews", "rows": 30_000_000, "ddl_cols": "review_id BIGINT, product_id BIGINT, customer_id BIGINT, rating INT, title STRING, review_date DATE, verified_purchase BOOLEAN, helpful_votes INT, status STRING",
         "insert_expr": "id + {offset} AS review_id, floor(rand()*500000)+1 AS product_id, floor(rand()*2000000)+1 AS customer_id, cast(floor(rand()*5)+1 as INT) AS rating, concat(element_at(array('Great','Good','Okay','Bad','Terrible','Amazing','Perfect','Decent','Poor','Excellent'),cast(floor(rand()*10)+1 as INT)),' product') AS title, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS review_date, rand() > 0.3 AS verified_purchase, cast(floor(rand()*100) as INT) AS helpful_votes, element_at(array('published','pending','rejected','flagged'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "orders", "rows": 5_000_000, "ddl_cols": "order_id BIGINT, customer_id BIGINT, order_date DATE, total_amount DECIMAL(10,2), shipping_cost DECIMAL(8,2), tax DECIMAL(8,2), payment_method STRING, shipping_method STRING, status STRING, promo_code STRING",
         "insert_expr": "id + {offset} AS order_id, floor(rand()*2000000)+1 AS customer_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS order_date, round(rand()*2000+10,2) AS total_amount, round(rand()*30,2) AS shipping_cost, round(rand()*200,2) AS tax, element_at(array('credit_card','debit_card','paypal','apple_pay','google_pay','gift_card'),cast(floor(rand()*6)+1 as INT)) AS payment_method, element_at(array('standard','express','next_day','same_day','pickup'),cast(floor(rand()*5)+1 as INT)) AS shipping_method, element_at(array('completed','processing','shipped','returned','cancelled'),cast(floor(rand()*5)+1 as INT)) AS status, CASE WHEN rand() > 0.8 THEN concat('PROMO',floor(rand()*100)) ELSE NULL END AS promo_code"},
        {"name": "cart_events", "rows": 5_000_000, "ddl_cols": "event_id BIGINT, session_id STRING, customer_id BIGINT, product_id BIGINT, action STRING, quantity INT, event_timestamp TIMESTAMP",
         "insert_expr": "id + {offset} AS event_id, concat('sess-',substring(sha2(cast(id as STRING),256),1,12)) AS session_id, floor(rand()*2000000)+1 AS customer_id, floor(rand()*500000)+1 AS product_id, element_at(array('add','remove','update_qty','save_for_later','move_to_cart'),cast(floor(rand()*5)+1 as INT)) AS action, cast(floor(rand()*5)+1 as INT) AS quantity, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp"},
        {"name": "customers", "rows": 1_000_000, "ddl_cols": "customer_id BIGINT, first_name STRING, last_name STRING, email STRING, phone STRING, address STRING, city STRING, state STRING, zip STRING, loyalty_tier STRING, created_date DATE",
         "insert_expr": "id + {offset} AS customer_id, element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, concat('shopper',id + {offset},'@example.com') AS email, concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone, concat(cast(floor(rand()*9999)+1 as STRING),' Market St') AS address, element_at(array('New York','LA','Chicago','Houston','Phoenix'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ'),cast(floor(rand()*5)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip, element_at(array('bronze','silver','gold','platinum','diamond'),cast(floor(rand()*5)+1 as INT)) AS loyalty_tier, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS created_date"},
        {"name": "products", "rows": 1_000_000, "ddl_cols": "product_id BIGINT, name STRING, category STRING, subcategory STRING, brand STRING, price DECIMAL(10,2), cost DECIMAL(10,2), weight_kg DECIMAL(6,2), is_active BOOLEAN, created_date DATE",
         "insert_expr": "id + {offset} AS product_id, concat(element_at(array('Premium','Classic','Ultra','Pro','Essential','Deluxe','Basic','Advanced','Elite','Signature'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Widget','Gadget','Device','Tool','Accessory','Kit','Set','Pack','Bundle','System'),cast(floor(rand()*10)+1 as INT))) AS name, element_at(array('Electronics','Clothing','Home','Sports','Books','Beauty','Toys','Food','Garden','Auto'),cast(floor(rand()*10)+1 as INT)) AS category, element_at(array('Phones','Laptops','Shirts','Pants','Kitchen','Fitness','Fiction','Skincare','Outdoor','Parts'),cast(floor(rand()*10)+1 as INT)) AS subcategory, element_at(array('Apple','Samsung','Nike','Adidas','Sony','LG','HP','Dell','Canon','Bose'),cast(floor(rand()*10)+1 as INT)) AS brand, round(rand()*999+1,2) AS price, round(rand()*500+0.5,2) AS cost, round(rand()*20+0.1,2) AS weight_kg, rand() > 0.1 AS is_active, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS created_date"},
        {"name": "stores", "rows": 1_000_000, "ddl_cols": "store_id BIGINT, name STRING, store_type STRING, address STRING, city STRING, state STRING, zip STRING, manager STRING, sqft INT, opened_date DATE",
         "insert_expr": "id + {offset} AS store_id, concat(element_at(array('Downtown','Mall','Outlet','Express','Super','Mega','Mini','Hub','Central','Depot'),cast(floor(rand()*10)+1 as INT)),' Store #',id) AS name, element_at(array('flagship','standard','outlet','express','popup','warehouse'),cast(floor(rand()*6)+1 as INT)) AS store_type, concat(cast(floor(rand()*9999)+1 as STRING),' Retail Blvd') AS address, element_at(array('New York','LA','Chicago','Houston','Phoenix'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ'),cast(floor(rand()*5)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip, concat('Manager ',id) AS manager, cast(floor(rand()*50000)+1000 as INT) AS sqft, date_add('2000-01-01',cast(floor(rand()*9125) as INT)) AS opened_date"},
        {"name": "inventory", "rows": 1_000_000, "ddl_cols": "inventory_id BIGINT, product_id BIGINT, warehouse_id BIGINT, quantity_on_hand INT, quantity_reserved INT, reorder_point INT, last_received DATE, last_counted DATE",
         "insert_expr": "id + {offset} AS inventory_id, floor(rand()*500000)+1 AS product_id, floor(rand()*100)+1 AS warehouse_id, cast(floor(rand()*10000) as INT) AS quantity_on_hand, cast(floor(rand()*500) as INT) AS quantity_reserved, cast(floor(rand()*100)+10 as INT) AS reorder_point, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS last_received, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS last_counted"},
        {"name": "categories", "rows": 1_000_000, "ddl_cols": "category_id BIGINT, name STRING, parent_category STRING, description STRING, is_active BOOLEAN, display_order INT",
         "insert_expr": "id + {offset} AS category_id, concat('Category ',id + {offset}) AS name, element_at(array('Electronics','Clothing','Home','Sports','Books','Beauty','Toys','Food','Garden','Auto'),cast(floor(rand()*10)+1 as INT)) AS parent_category, concat('Description for category ',id + {offset}) AS description, rand() > 0.1 AS is_active, cast(floor(rand()*1000) as INT) AS display_order"},
        {"name": "promotions", "rows": 500_000, "ddl_cols": "promo_id BIGINT, code STRING, discount_type STRING, discount_value DECIMAL(8,2), start_date DATE, end_date DATE, min_purchase DECIMAL(8,2), max_uses INT, used_count INT",
         "insert_expr": "id + {offset} AS promo_id, concat('PROMO',lpad(cast(id + {offset} as STRING),6,'0')) AS code, element_at(array('percentage','fixed','bogo','free_shipping'),cast(floor(rand()*4)+1 as INT)) AS discount_type, round(rand()*50+5,2) AS discount_value, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS start_date, date_add('2024-06-01',cast(floor(rand()*365) as INT)) AS end_date, round(rand()*100,2) AS min_purchase, cast(floor(rand()*10000)+100 as INT) AS max_uses, cast(floor(rand()*5000) as INT) AS used_count"},
        {"name": "warehouses", "rows": 500_000, "ddl_cols": "warehouse_id BIGINT, name STRING, address STRING, city STRING, state STRING, capacity_sqft INT, current_utilization DECIMAL(5,2), manager STRING",
         "insert_expr": "id + {offset} AS warehouse_id, concat('Warehouse ',element_at(array('Alpha','Beta','Gamma','Delta','Epsilon'),cast(floor(rand()*5)+1 as INT)),'-',id) AS name, concat(cast(floor(rand()*9999)+1 as STRING),' Logistics Way') AS address, element_at(array('Memphis','Dallas','Chicago','Louisville','Indianapolis'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('TN','TX','IL','KY','IN'),cast(floor(rand()*5)+1 as INT)) AS state, cast(floor(rand()*500000)+10000 as INT) AS capacity_sqft, round(rand()*100,2) AS current_utilization, concat('WH Manager ',id) AS manager"},
        {"name": "suppliers", "rows": 200_000, "ddl_cols": "supplier_id BIGINT, name STRING, contact_email STRING, country STRING, lead_time_days INT, rating DECIMAL(3,1), category STRING",
         "insert_expr": "id + {offset} AS supplier_id, concat(element_at(array('Global','Pacific','Atlantic','Northern','Southern','Eastern','Western','Central','Premier','United'),cast(floor(rand()*10)+1 as INT)),' Supplies Inc') AS name, concat('supplier',id + {offset},'@example.com') AS contact_email, element_at(array('US','CN','DE','JP','KR','TW','MX','IN','VN','TH'),cast(floor(rand()*10)+1 as INT)) AS country, cast(floor(rand()*60)+3 as INT) AS lead_time_days, round(rand()*4+1,1) AS rating, element_at(array('electronics','textiles','packaging','raw_materials','components'),cast(floor(rand()*5)+1 as INT)) AS category"},
        {"name": "shipping", "rows": 300_000, "ddl_cols": "shipment_id BIGINT, order_id BIGINT, carrier STRING, tracking_number STRING, shipped_date DATE, delivered_date DATE, status STRING, cost DECIMAL(8,2)",
         "insert_expr": "id + {offset} AS shipment_id, floor(rand()*5000000)+1 AS order_id, element_at(array('UPS','FedEx','USPS','DHL','Amazon Logistics'),cast(floor(rand()*5)+1 as INT)) AS carrier, upper(substring(sha2(cast(id as STRING),256),1,16)) AS tracking_number, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS shipped_date, date_add('2020-01-01',cast(floor(rand()*1825)+5 as INT)) AS delivered_date, element_at(array('delivered','in_transit','out_for_delivery','returned','exception'),cast(floor(rand()*5)+1 as INT)) AS status, round(rand()*50+3,2) AS cost"},
        {"name": "returns", "rows": 200_000, "ddl_cols": "return_id BIGINT, order_id BIGINT, product_id BIGINT, reason STRING, return_date DATE, refund_amount DECIMAL(10,2), condition_received STRING, status STRING",
         "insert_expr": "id + {offset} AS return_id, floor(rand()*5000000)+1 AS order_id, floor(rand()*500000)+1 AS product_id, element_at(array('defective','wrong_item','not_as_described','changed_mind','too_late','damaged_shipping'),cast(floor(rand()*6)+1 as INT)) AS reason, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS return_date, round(rand()*500+5,2) AS refund_amount, element_at(array('new','opened','damaged','used'),cast(floor(rand()*4)+1 as INT)) AS condition_received, element_at(array('approved','pending','denied','processed'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "wishlists", "rows": 300_000, "ddl_cols": "wishlist_id BIGINT, customer_id BIGINT, product_id BIGINT, added_date DATE, priority STRING, is_public BOOLEAN",
         "insert_expr": "id + {offset} AS wishlist_id, floor(rand()*2000000)+1 AS customer_id, floor(rand()*500000)+1 AS product_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS added_date, element_at(array('high','medium','low'),cast(floor(rand()*3)+1 as INT)) AS priority, rand() > 0.7 AS is_public"},
        {"name": "loyalty_points", "rows": 200_000, "ddl_cols": "txn_id BIGINT, customer_id BIGINT, points INT, txn_type STRING, txn_date DATE, order_id BIGINT, expiry_date DATE",
         "insert_expr": "id + {offset} AS txn_id, floor(rand()*2000000)+1 AS customer_id, cast(floor(rand()*5000)-500 as INT) AS points, element_at(array('earned','redeemed','bonus','expired','adjusted'),cast(floor(rand()*5)+1 as INT)) AS txn_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS txn_date, floor(rand()*5000000)+1 AS order_id, date_add('2025-01-01',cast(floor(rand()*730) as INT)) AS expiry_date"},
        {"name": "price_history", "rows": 500_000, "ddl_cols": "history_id BIGINT, product_id BIGINT, old_price DECIMAL(10,2), new_price DECIMAL(10,2), change_date DATE, reason STRING",
         "insert_expr": "id + {offset} AS history_id, floor(rand()*500000)+1 AS product_id, round(rand()*999+1,2) AS old_price, round(rand()*999+1,2) AS new_price, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS change_date, element_at(array('seasonal','clearance','competitor','cost_change','promotion','new_launch'),cast(floor(rand()*6)+1 as INT)) AS reason"},
        {"name": "gift_cards", "rows": 100_000, "ddl_cols": "card_id BIGINT, card_number STRING, original_balance DECIMAL(8,2), current_balance DECIMAL(8,2), purchased_date DATE, expiry_date DATE, status STRING",
         "insert_expr": "id + {offset} AS card_id, upper(substring(sha2(cast(id as STRING),256),1,16)) AS card_number, round(rand()*200+10,2) AS original_balance, round(rand()*200,2) AS current_balance, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS purchased_date, date_add('2025-01-01',cast(floor(rand()*730) as INT)) AS expiry_date, element_at(array('active','used','expired','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status"},
    ],
    "views": [
        ("v_order_summary", "SELECT o.order_id, o.customer_id, o.order_date, o.total_amount, count(oi.item_id) AS items FROM {s}.orders o JOIN {s}.order_items oi ON o.order_id = oi.order_id GROUP BY o.order_id, o.customer_id, o.order_date, o.total_amount"),
        ("v_product_performance", "SELECT p.product_id, p.name, p.category, count(oi.item_id) AS units_sold, sum(oi.total) AS revenue, avg(r.rating) AS avg_rating FROM {s}.products p LEFT JOIN {s}.order_items oi ON p.product_id = oi.product_id LEFT JOIN {s}.reviews r ON p.product_id = r.product_id GROUP BY p.product_id, p.name, p.category"),
        ("v_customer_ltv", "SELECT c.customer_id, c.first_name, c.last_name, c.loyalty_tier, count(DISTINCT o.order_id) AS orders, sum(o.total_amount) AS lifetime_value FROM {s}.customers c JOIN {s}.orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name, c.last_name, c.loyalty_tier"),
        ("v_monthly_revenue", "SELECT date_trunc('month',order_date) AS month, count(*) AS orders, sum(total_amount) AS revenue, avg(total_amount) AS aov FROM {s}.orders GROUP BY date_trunc('month',order_date)"),
        ("v_inventory_alerts", "SELECT i.*, p.name, p.category FROM {s}.inventory i JOIN {s}.products p ON i.product_id = p.product_id WHERE i.quantity_on_hand < i.reorder_point"),
        ("v_top_reviewers", "SELECT customer_id, count(*) AS review_count, avg(rating) AS avg_rating FROM {s}.reviews WHERE verified_purchase = true GROUP BY customer_id HAVING count(*) > 5"),
        ("v_conversion_funnel", "SELECT event_type, count(*) AS events FROM {s}.clickstream GROUP BY event_type"),
        ("v_return_rate", "SELECT p.category, count(DISTINCT ret.return_id) AS returns, count(DISTINCT oi.item_id) AS sold, round(count(DISTINCT ret.return_id)*100.0/count(DISTINCT oi.item_id),2) AS return_pct FROM {s}.products p JOIN {s}.order_items oi ON p.product_id = oi.product_id LEFT JOIN {s}.returns ret ON oi.order_id = ret.order_id AND oi.product_id = ret.product_id GROUP BY p.category"),
        ("v_shipping_performance", "SELECT carrier, avg(datediff(delivered_date,shipped_date)) AS avg_delivery_days, count(*) AS shipments FROM {s}.shipping WHERE status = 'delivered' GROUP BY carrier"),
        ("v_promo_effectiveness", "SELECT pr.code, pr.discount_type, pr.used_count, count(o.order_id) AS orders_with_promo FROM {s}.promotions pr LEFT JOIN {s}.orders o ON o.promo_code = pr.code GROUP BY pr.code, pr.discount_type, pr.used_count"),
        ("v_store_performance", "SELECT s.name, s.city, s.store_type, s.sqft FROM {s}.stores s"),
        ("v_category_trends", "SELECT p.category, date_trunc('month',oi.shipped_date) AS month, sum(oi.total) AS revenue FROM {s}.order_items oi JOIN {s}.products p ON oi.product_id = p.product_id GROUP BY p.category, date_trunc('month',oi.shipped_date)"),
        ("v_supplier_lead_times", "SELECT s.name, s.country, s.lead_time_days, s.rating FROM {s}.suppliers s ORDER BY s.lead_time_days"),
        ("v_warehouse_utilization", "SELECT w.name, w.city, w.capacity_sqft, w.current_utilization, count(i.inventory_id) AS products_stored FROM {s}.warehouses w LEFT JOIN {s}.inventory i ON w.warehouse_id = i.warehouse_id GROUP BY w.name, w.city, w.capacity_sqft, w.current_utilization"),
        ("v_loyalty_balance", "SELECT customer_id, sum(CASE WHEN txn_type IN ('earned','bonus') THEN points ELSE 0 END) AS earned, sum(CASE WHEN txn_type = 'redeemed' THEN abs(points) ELSE 0 END) AS redeemed FROM {s}.loyalty_points GROUP BY customer_id"),
        ("v_price_volatility", "SELECT product_id, count(*) AS changes, avg(abs(new_price-old_price)) AS avg_change, max(abs(new_price-old_price)) AS max_change FROM {s}.price_history GROUP BY product_id"),
        ("v_gift_card_liability", "SELECT status, count(*) AS cards, sum(current_balance) AS total_liability FROM {s}.gift_cards GROUP BY status"),
        ("v_wishlist_demand", "SELECT product_id, count(*) AS wishlist_count FROM {s}.wishlists GROUP BY product_id ORDER BY wishlist_count DESC"),
        ("v_device_analytics", "SELECT device_type, count(*) AS events, count(DISTINCT session_id) AS sessions FROM {s}.clickstream GROUP BY device_type"),
        ("v_customer_segments", "SELECT loyalty_tier, count(*) AS customers, avg(datediff(current_date(),created_date)) AS avg_tenure_days FROM {s}.customers GROUP BY loyalty_tier"),
    ],
    "udfs": [
        ("mask_email_retail", "email STRING", "STRING", "Masks customer email", "concat(left(email,2),'***@',split(email,'@')[1])"),
        ("mask_phone_retail", "phone STRING", "STRING", "Masks phone number", "concat('***-***-',right(phone,4))"),
        ("format_price", "price DECIMAL(10,2)", "STRING", "Formats price with dollar sign", "concat('$',format_number(price,2))"),
        ("calculate_discount_pct", "original DECIMAL(10,2), discounted DECIMAL(10,2)", "STRING", "Calculates discount percentage", "concat(cast(round((1 - discounted/original)*100,0) as STRING),'% off')"),
        ("loyalty_tier_level", "tier STRING", "INT", "Numeric level for loyalty tier", "CASE WHEN tier = 'diamond' THEN 5 WHEN tier = 'platinum' THEN 4 WHEN tier = 'gold' THEN 3 WHEN tier = 'silver' THEN 2 ELSE 1 END"),
        ("shipping_eta", "method STRING", "STRING", "Estimated delivery time", "CASE WHEN method = 'same_day' THEN 'Today' WHEN method = 'next_day' THEN '1 business day' WHEN method = 'express' THEN '2-3 business days' WHEN method = 'standard' THEN '5-7 business days' ELSE '7-14 business days' END"),
        ("product_margin", "price DECIMAL(10,2), cost DECIMAL(10,2)", "DECIMAL(5,2)", "Calculates product margin percentage", "round((price - cost) / price * 100, 2)"),
        ("rating_stars", "rating INT", "STRING", "Converts rating to star display", "repeat('★', rating) || repeat('☆', 5 - rating)"),
        ("order_status_label", "status STRING", "STRING", "Human-readable order status", "initcap(replace(status,'_',' '))"),
        ("is_high_value_order", "amount DECIMAL(10,2)", "BOOLEAN", "Checks if order is high value", "amount > 500"),
        ("days_since_order", "order_date DATE", "INT", "Days since order was placed", "datediff(current_date(), order_date)"),
        ("return_eligibility", "order_date DATE", "BOOLEAN", "Checks if within 30-day return window", "datediff(current_date(), order_date) <= 30"),
        ("cart_abandonment_risk", "events INT, duration INT", "STRING", "Risk of cart abandonment", "CASE WHEN events < 3 AND duration > 300 THEN 'High' WHEN events < 5 THEN 'Medium' ELSE 'Low' END"),
        ("format_tracking", "carrier STRING, tracking STRING", "STRING", "Formats tracking display", "concat(carrier, ': ', tracking)"),
        ("inventory_status", "on_hand INT, reorder_point INT", "STRING", "Inventory status label", "CASE WHEN on_hand = 0 THEN 'Out of Stock' WHEN on_hand < reorder_point THEN 'Low Stock' WHEN on_hand < reorder_point * 3 THEN 'Adequate' ELSE 'Overstocked' END"),
        ("promo_discount_display", "discount_type STRING, value DECIMAL(8,2)", "STRING", "Display promo discount", "CASE WHEN discount_type = 'percentage' THEN concat(cast(value as STRING),'% off') WHEN discount_type = 'fixed' THEN concat('$',format_number(value,2),' off') WHEN discount_type = 'bogo' THEN 'Buy One Get One' ELSE 'Free Shipping' END"),
        ("gift_card_status_label", "status STRING, balance DECIMAL(8,2)", "STRING", "Gift card status display", "CASE WHEN status = 'expired' THEN 'Expired' WHEN balance = 0 THEN 'Used' ELSE concat('$',format_number(balance,2),' remaining') END"),
        ("category_icon", "category STRING", "STRING", "Emoji icon for category", "CASE WHEN category = 'Electronics' THEN '📱' WHEN category = 'Clothing' THEN '👕' WHEN category = 'Home' THEN '🏠' WHEN category = 'Sports' THEN '⚽' WHEN category = 'Books' THEN '📚' ELSE '🛍️' END"),
        ("review_sentiment", "rating INT", "STRING", "Simple sentiment from rating", "CASE WHEN rating >= 4 THEN 'positive' WHEN rating = 3 THEN 'neutral' ELSE 'negative' END"),
        ("anonymize_shopper", "first_name STRING, customer_id BIGINT", "STRING", "Anonymized shopper ID", "concat('SHOP-',substring(sha2(concat(first_name,cast(customer_id as STRING)),256),1,8))"),
    ],
}

INDUSTRIES["telecom"] = {
    "tables": [
        {"name": "cdr_records", "rows": 100_000_000, "ddl_cols": "cdr_id BIGINT, subscriber_id BIGINT, call_type STRING, called_number STRING, duration_seconds INT, start_time TIMESTAMP, end_time TIMESTAMP, cell_tower_id BIGINT, cost DECIMAL(8,4), status STRING",
         "insert_expr": "id + {offset} AS cdr_id, floor(rand()*2000000)+1 AS subscriber_id, element_at(array('voice_outgoing','voice_incoming','sms_outgoing','sms_incoming','mms','voicemail'),cast(floor(rand()*6)+1 as INT)) AS call_type, concat('+1555',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS called_number, cast(floor(rand()*3600) as INT) AS duration_seconds, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS start_time, dateadd(SECOND, cast(floor(rand()*1825*86400)+3600 as INT), '2020-01-01') AS end_time, floor(rand()*50000)+1 AS cell_tower_id, round(rand()*5,4) AS cost, element_at(array('completed','dropped','busy','no_answer','failed'),cast(floor(rand()*5)+1 as INT)) AS status"},
        {"name": "data_usage", "rows": 50_000_000, "ddl_cols": "usage_id BIGINT, subscriber_id BIGINT, session_start TIMESTAMP, session_end TIMESTAMP, bytes_uploaded BIGINT, bytes_downloaded BIGINT, app_category STRING, network_type STRING, cell_tower_id BIGINT, throttled BOOLEAN",
         "insert_expr": "id + {offset} AS usage_id, floor(rand()*2000000)+1 AS subscriber_id, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS session_start, dateadd(SECOND, cast(floor(rand()*1825*86400)+7200 as INT), '2020-01-01') AS session_end, cast(floor(rand()*1000000000) as BIGINT) AS bytes_uploaded, cast(floor(rand()*5000000000) as BIGINT) AS bytes_downloaded, element_at(array('streaming','social_media','browsing','gaming','email','cloud','voip','other'),cast(floor(rand()*8)+1 as INT)) AS app_category, element_at(array('5G','4G_LTE','3G','WiFi'),cast(floor(rand()*4)+1 as INT)) AS network_type, floor(rand()*50000)+1 AS cell_tower_id, rand() > 0.95 AS throttled"},
        {"name": "billing", "rows": 30_000_000, "ddl_cols": "bill_id BIGINT, subscriber_id BIGINT, billing_period DATE, base_charge DECIMAL(8,2), data_charge DECIMAL(8,2), overage_charge DECIMAL(8,2), taxes DECIMAL(8,2), total_amount DECIMAL(8,2), payment_status STRING, due_date DATE",
         "insert_expr": "id + {offset} AS bill_id, floor(rand()*2000000)+1 AS subscriber_id, date_add('2020-01-01',cast(floor(rand()*60)*30 as INT)) AS billing_period, round(rand()*80+20,2) AS base_charge, round(rand()*50,2) AS data_charge, round(rand()*30,2) AS overage_charge, round(rand()*15+2,2) AS taxes, round(rand()*175+22,2) AS total_amount, element_at(array('paid','pending','overdue','partial','credited'),cast(floor(rand()*5)+1 as INT)) AS payment_status, date_add('2020-01-01',cast(floor(rand()*60)*30+30 as INT)) AS due_date"},
        {"name": "network_events", "rows": 5_000_000, "ddl_cols": "event_id BIGINT, tower_id BIGINT, event_type STRING, severity STRING, start_time TIMESTAMP, end_time TIMESTAMP, affected_subscribers INT, root_cause STRING",
         "insert_expr": "id + {offset} AS event_id, floor(rand()*50000)+1 AS tower_id, element_at(array('outage','degradation','maintenance','upgrade','congestion','interference'),cast(floor(rand()*6)+1 as INT)) AS event_type, element_at(array('critical','major','minor','warning','info'),cast(floor(rand()*5)+1 as INT)) AS severity, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS start_time, dateadd(SECOND, cast(floor(rand()*1825*86400)+86400 as INT), '2020-01-01') AS end_time, cast(floor(rand()*10000) as INT) AS affected_subscribers, element_at(array('hardware_failure','software_bug','weather','power_outage','capacity','configuration'),cast(floor(rand()*6)+1 as INT)) AS root_cause"},
        {"name": "service_tickets", "rows": 5_000_000, "ddl_cols": "ticket_id BIGINT, subscriber_id BIGINT, category STRING, priority STRING, created_date DATE, resolved_date DATE, status STRING, channel STRING, satisfaction_score INT",
         "insert_expr": "id + {offset} AS ticket_id, floor(rand()*2000000)+1 AS subscriber_id, element_at(array('billing','network','device','plan_change','cancellation','technical','general'),cast(floor(rand()*7)+1 as INT)) AS category, element_at(array('critical','high','medium','low'),cast(floor(rand()*4)+1 as INT)) AS priority, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS created_date, date_add('2020-01-01',cast(floor(rand()*1825)+3 as INT)) AS resolved_date, element_at(array('open','in_progress','resolved','closed','escalated'),cast(floor(rand()*5)+1 as INT)) AS status, element_at(array('phone','chat','email','store','app','social'),cast(floor(rand()*6)+1 as INT)) AS channel, cast(floor(rand()*5)+1 as INT) AS satisfaction_score"},
        {"name": "subscribers", "rows": 1_000_000, "ddl_cols": "subscriber_id BIGINT, first_name STRING, last_name STRING, email STRING, phone STRING, plan_id BIGINT, activation_date DATE, status STRING, credit_class STRING, contract_end DATE",
         "insert_expr": "id + {offset} AS subscriber_id, element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, concat('sub',id + {offset},'@example.com') AS email, concat('+1555',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone, floor(rand()*50)+1 AS plan_id, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS activation_date, element_at(array('active','suspended','cancelled','pending'),cast(floor(rand()*4)+1 as INT)) AS status, element_at(array('A','B','C','D'),cast(floor(rand()*4)+1 as INT)) AS credit_class, date_add('2025-01-01',cast(floor(rand()*730) as INT)) AS contract_end"},
        {"name": "plans", "rows": 1_000_000, "ddl_cols": "plan_id BIGINT, name STRING, plan_type STRING, monthly_cost DECIMAL(8,2), data_limit_gb INT, voice_minutes INT, sms_limit INT, is_unlimited BOOLEAN, is_active BOOLEAN",
         "insert_expr": "id + {offset} AS plan_id, concat(element_at(array('Basic','Standard','Premium','Ultimate','Family','Business','Student','Senior','Unlimited','Flex'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('5G','Plus','Pro','Lite','Max'),cast(floor(rand()*5)+1 as INT))) AS name, element_at(array('postpaid','prepaid','business','family','mvno'),cast(floor(rand()*5)+1 as INT)) AS plan_type, round(rand()*150+15,2) AS monthly_cost, element_at(array(2,5,10,25,50,100,-1),cast(floor(rand()*7)+1 as INT)) AS data_limit_gb, element_at(array(100,500,1000,2000,-1),cast(floor(rand()*5)+1 as INT)) AS voice_minutes, element_at(array(100,500,1000,-1),cast(floor(rand()*4)+1 as INT)) AS sms_limit, rand() > 0.6 AS is_unlimited, rand() > 0.1 AS is_active"},
        {"name": "towers", "rows": 1_000_000, "ddl_cols": "tower_id BIGINT, tower_name STRING, latitude DOUBLE, longitude DOUBLE, tower_type STRING, technology STRING, capacity INT, city STRING, state STRING, status STRING",
         "insert_expr": "id + {offset} AS tower_id, concat('TWR-',lpad(cast(id + {offset} as STRING),6,'0')) AS tower_name, round(rand()*25+25,6) AS latitude, round(rand()*55-125,6) AS longitude, element_at(array('macro','micro','pico','femto','small_cell'),cast(floor(rand()*5)+1 as INT)) AS tower_type, element_at(array('5G_NR','4G_LTE','3G_UMTS','5G_mmWave'),cast(floor(rand()*4)+1 as INT)) AS technology, cast(floor(rand()*5000)+100 as INT) AS capacity, element_at(array('New York','LA','Chicago','Houston','Phoenix'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ'),cast(floor(rand()*5)+1 as INT)) AS state, element_at(array('active','maintenance','planned','decommissioned'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "devices", "rows": 1_000_000, "ddl_cols": "device_id BIGINT, subscriber_id BIGINT, imei STRING, make STRING, model STRING, os STRING, os_version STRING, activation_date DATE, is_5g_capable BOOLEAN",
         "insert_expr": "id + {offset} AS device_id, floor(rand()*2000000)+1 AS subscriber_id, lpad(cast(floor(rand()*999999999999999) as STRING),15,'0') AS imei, element_at(array('Apple','Samsung','Google','OnePlus','Motorola','Nokia','Sony','LG','Huawei','Xiaomi'),cast(floor(rand()*10)+1 as INT)) AS make, element_at(array('Pro Max','Ultra','Plus','Lite','Standard','SE','Mini','Edge','Fold','Flip'),cast(floor(rand()*10)+1 as INT)) AS model, element_at(array('iOS','Android'),cast(floor(rand()*2)+1 as INT)) AS os, element_at(array('17','16','15','14','13'),cast(floor(rand()*5)+1 as INT)) AS os_version, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS activation_date, rand() > 0.3 AS is_5g_capable"},
        {"name": "roaming_usage", "rows": 1_000_000, "ddl_cols": "roaming_id BIGINT, subscriber_id BIGINT, country STRING, roaming_type STRING, data_mb DECIMAL(10,2), voice_minutes INT, cost DECIMAL(8,2), usage_date DATE",
         "insert_expr": "id + {offset} AS roaming_id, floor(rand()*2000000)+1 AS subscriber_id, element_at(array('CA','MX','UK','DE','FR','JP','AU','BR','IN','KR'),cast(floor(rand()*10)+1 as INT)) AS country, element_at(array('data','voice','sms','mms'),cast(floor(rand()*4)+1 as INT)) AS roaming_type, round(rand()*5000,2) AS data_mb, cast(floor(rand()*120) as INT) AS voice_minutes, round(rand()*100,2) AS cost, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS usage_date"},
        {"name": "number_portability", "rows": 500_000, "ddl_cols": "port_id BIGINT, phone_number STRING, from_carrier STRING, to_carrier STRING, request_date DATE, completion_date DATE, status STRING",
         "insert_expr": "id + {offset} AS port_id, concat('+1555',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone_number, element_at(array('AT&T','Verizon','T-Mobile','Sprint','US Cellular'),cast(floor(rand()*5)+1 as INT)) AS from_carrier, element_at(array('AT&T','Verizon','T-Mobile','Sprint','US Cellular'),cast(floor(rand()*5)+1 as INT)) AS to_carrier, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS request_date, date_add('2020-01-01',cast(floor(rand()*1825)+3 as INT)) AS completion_date, element_at(array('completed','pending','rejected','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "promotions_telecom", "rows": 500_000, "ddl_cols": "promo_id BIGINT, name STRING, promo_type STRING, discount_amount DECIMAL(8,2), start_date DATE, end_date DATE, eligibility STRING, redemptions INT",
         "insert_expr": "id + {offset} AS promo_id, concat(element_at(array('Spring','Summer','Fall','Winter','Holiday','Back2School','New Year','Black Friday','Cyber Monday','Flash'),cast(floor(rand()*10)+1 as INT)),' Deal') AS name, element_at(array('monthly_credit','data_bonus','device_discount','family_add','upgrade','loyalty'),cast(floor(rand()*6)+1 as INT)) AS promo_type, round(rand()*50+5,2) AS discount_amount, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS start_date, date_add('2024-06-01',cast(floor(rand()*365) as INT)) AS end_date, element_at(array('new_customer','existing','upgrade_eligible','family_plan','any'),cast(floor(rand()*5)+1 as INT)) AS eligibility, cast(floor(rand()*50000) as INT) AS redemptions"},
        {"name": "coverage_areas", "rows": 200_000, "ddl_cols": "area_id BIGINT, zip_code STRING, city STRING, state STRING, coverage_type STRING, signal_strength INT, technology STRING, last_tested DATE",
         "insert_expr": "id + {offset} AS area_id, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip_code, element_at(array('New York','LA','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','Austin'),cast(floor(rand()*10)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ','PA','TX','CA','TX','TX'),cast(floor(rand()*10)+1 as INT)) AS state, element_at(array('indoor','outdoor','deep_indoor','suburban','rural'),cast(floor(rand()*5)+1 as INT)) AS coverage_type, cast(floor(rand()*5)+1 as INT) AS signal_strength, element_at(array('5G','4G','3G'),cast(floor(rand()*3)+1 as INT)) AS technology, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS last_tested"},
        {"name": "tower_maintenance", "rows": 300_000, "ddl_cols": "maintenance_id BIGINT, tower_id BIGINT, maintenance_type STRING, scheduled_date DATE, completed_date DATE, cost DECIMAL(10,2), technician STRING, status STRING",
         "insert_expr": "id + {offset} AS maintenance_id, floor(rand()*50000)+1 AS tower_id, element_at(array('preventive','corrective','upgrade','inspection','emergency'),cast(floor(rand()*5)+1 as INT)) AS maintenance_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS scheduled_date, date_add('2020-01-01',cast(floor(rand()*1825)+1 as INT)) AS completed_date, round(rand()*50000+500,2) AS cost, concat('Tech-',floor(rand()*500)+1) AS technician, element_at(array('completed','scheduled','in_progress','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "sim_cards", "rows": 200_000, "ddl_cols": "sim_id BIGINT, iccid STRING, subscriber_id BIGINT, sim_type STRING, activation_date DATE, status STRING, puk_hash STRING",
         "insert_expr": "id + {offset} AS sim_id, lpad(cast(floor(rand()*99999999999999999) as STRING),19,'0') AS iccid, floor(rand()*2000000)+1 AS subscriber_id, element_at(array('physical','eSIM','nano','micro'),cast(floor(rand()*4)+1 as INT)) AS sim_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS activation_date, element_at(array('active','deactivated','suspended','lost'),cast(floor(rand()*4)+1 as INT)) AS status, sha2(cast(id as STRING),256) AS puk_hash"},
        {"name": "churn_predictions", "rows": 200_000, "ddl_cols": "prediction_id BIGINT, subscriber_id BIGINT, churn_probability DOUBLE, risk_factors STRING, predicted_date DATE, model_version STRING",
         "insert_expr": "id + {offset} AS prediction_id, floor(rand()*2000000)+1 AS subscriber_id, round(rand(),4) AS churn_probability, element_at(array('high_usage_drop','late_payments','no_contract','competitor_offer','service_complaints','data_limit_exceeded'),cast(floor(rand()*6)+1 as INT)) AS risk_factors, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS predicted_date, element_at(array('v1.0','v1.1','v2.0','v2.1'),cast(floor(rand()*4)+1 as INT)) AS model_version"},
        {"name": "value_added_services", "rows": 300_000, "ddl_cols": "service_id BIGINT, subscriber_id BIGINT, service_name STRING, monthly_fee DECIMAL(6,2), activated_date DATE, status STRING",
         "insert_expr": "id + {offset} AS service_id, floor(rand()*2000000)+1 AS subscriber_id, element_at(array('voicemail_plus','caller_id','call_waiting','data_shield','family_locator','cloud_storage','music_streaming','video_streaming','insurance','hotspot'),cast(floor(rand()*10)+1 as INT)) AS service_name, round(rand()*15+2,2) AS monthly_fee, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS activated_date, element_at(array('active','cancelled','trial','suspended'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "spectrum_allocation", "rows": 100_000, "ddl_cols": "allocation_id BIGINT, band STRING, frequency_mhz INT, bandwidth_mhz INT, region STRING, license_expiry DATE, technology STRING",
         "insert_expr": "id + {offset} AS allocation_id, element_at(array('n71','n41','n77','n78','n260','n261','Band 2','Band 4','Band 12','Band 66'),cast(floor(rand()*10)+1 as INT)) AS band, element_at(array(600,700,850,1900,2500,3500,28000,39000),cast(floor(rand()*8)+1 as INT)) AS frequency_mhz, element_at(array(5,10,15,20,40,60,100),cast(floor(rand()*7)+1 as INT)) AS bandwidth_mhz, element_at(array('Northeast','Southeast','Midwest','Southwest','West','Pacific'),cast(floor(rand()*6)+1 as INT)) AS region, date_add('2025-01-01',cast(floor(rand()*3650) as INT)) AS license_expiry, element_at(array('5G_NR','4G_LTE','5G_mmWave'),cast(floor(rand()*3)+1 as INT)) AS technology"},
    ],
    "views": [
        ("v_subscriber_usage", "SELECT s.subscriber_id, s.first_name, s.last_name, count(DISTINCT c.cdr_id) AS calls, sum(c.duration_seconds) AS total_duration, sum(d.bytes_downloaded) AS total_download FROM {s}.subscribers s LEFT JOIN {s}.cdr_records c ON s.subscriber_id = c.subscriber_id LEFT JOIN {s}.data_usage d ON s.subscriber_id = d.subscriber_id GROUP BY s.subscriber_id, s.first_name, s.last_name"),
        ("v_monthly_revenue_telecom", "SELECT date_trunc('month',billing_period) AS month, count(*) AS bills, sum(total_amount) AS revenue, avg(total_amount) AS arpu FROM {s}.billing GROUP BY date_trunc('month',billing_period)"),
        ("v_network_health", "SELECT t.tower_id, t.tower_name, t.technology, count(ne.event_id) AS incidents, sum(ne.affected_subscribers) AS total_affected FROM {s}.towers t LEFT JOIN {s}.network_events ne ON t.tower_id = ne.tower_id GROUP BY t.tower_id, t.tower_name, t.technology"),
        ("v_churn_risk", "SELECT s.subscriber_id, s.first_name, s.last_name, s.plan_id, cp.churn_probability, cp.risk_factors FROM {s}.subscribers s JOIN {s}.churn_predictions cp ON s.subscriber_id = cp.subscriber_id WHERE cp.churn_probability > 0.7"),
        ("v_data_consumption", "SELECT app_category, network_type, count(*) AS sessions, sum(bytes_downloaded)/1073741824 AS total_gb FROM {s}.data_usage GROUP BY app_category, network_type"),
        ("v_billing_overdue", "SELECT b.*, s.first_name, s.last_name, s.email FROM {s}.billing b JOIN {s}.subscribers s ON b.subscriber_id = s.subscriber_id WHERE b.payment_status = 'overdue'"),
        ("v_tower_load", "SELECT tower_id, date_trunc('hour',start_time) AS hour, count(*) AS calls, sum(duration_seconds) AS total_seconds FROM {s}.cdr_records GROUP BY tower_id, date_trunc('hour',start_time)"),
        ("v_device_distribution", "SELECT make, model, os, count(*) AS devices, sum(CASE WHEN is_5g_capable THEN 1 ELSE 0 END) AS five_g_capable FROM {s}.devices GROUP BY make, model, os"),
        ("v_roaming_revenue", "SELECT country, roaming_type, count(*) AS events, sum(cost) AS revenue FROM {s}.roaming_usage GROUP BY country, roaming_type"),
        ("v_plan_popularity", "SELECT p.name, p.plan_type, p.monthly_cost, count(s.subscriber_id) AS subscribers FROM {s}.plans p LEFT JOIN {s}.subscribers s ON p.plan_id = s.plan_id GROUP BY p.name, p.plan_type, p.monthly_cost"),
        ("v_ticket_resolution", "SELECT category, priority, avg(datediff(resolved_date,created_date)) AS avg_resolution_days, avg(satisfaction_score) AS avg_satisfaction FROM {s}.service_tickets WHERE status IN ('resolved','closed') GROUP BY category, priority"),
        ("v_port_in_out", "SELECT to_carrier, from_carrier, count(*) AS ports, status FROM {s}.number_portability GROUP BY to_carrier, from_carrier, status"),
        ("v_coverage_gaps", "SELECT city, state, technology, avg(signal_strength) AS avg_signal FROM {s}.coverage_areas WHERE signal_strength <= 2 GROUP BY city, state, technology"),
        ("v_maintenance_costs", "SELECT date_trunc('month',scheduled_date) AS month, maintenance_type, count(*) AS jobs, sum(cost) AS total_cost FROM {s}.tower_maintenance GROUP BY date_trunc('month',scheduled_date), maintenance_type"),
        ("v_vas_revenue", "SELECT service_name, count(*) AS subscribers, sum(monthly_fee) AS monthly_revenue FROM {s}.value_added_services WHERE status = 'active' GROUP BY service_name"),
        ("v_sim_inventory", "SELECT sim_type, status, count(*) AS sims FROM {s}.sim_cards GROUP BY sim_type, status"),
        ("v_call_quality", "SELECT status, count(*) AS calls, avg(duration_seconds) AS avg_duration FROM {s}.cdr_records GROUP BY status"),
        ("v_spectrum_utilization", "SELECT band, technology, sum(bandwidth_mhz) AS total_bandwidth FROM {s}.spectrum_allocation GROUP BY band, technology"),
        ("v_promo_performance", "SELECT name, promo_type, discount_amount, redemptions FROM {s}.promotions_telecom ORDER BY redemptions DESC"),
        ("v_subscriber_360", "SELECT s.subscriber_id, s.first_name, s.last_name, s.status, p.name AS plan, d.make AS device_make, d.model AS device_model FROM {s}.subscribers s LEFT JOIN {s}.plans p ON s.plan_id = p.plan_id LEFT JOIN {s}.devices d ON s.subscriber_id = d.subscriber_id"),
    ],
    "udfs": [
        ("mask_phone_telecom", "phone STRING", "STRING", "Masks phone number", "concat('+1***',right(phone,4))"),
        ("mask_imei", "imei STRING", "STRING", "Masks IMEI number", "concat('*****',right(imei,4))"),
        ("format_data_size", "bytes BIGINT", "STRING", "Formats bytes to human-readable", "CASE WHEN bytes >= 1073741824 THEN concat(round(bytes/1073741824,2),' GB') WHEN bytes >= 1048576 THEN concat(round(bytes/1048576,2),' MB') WHEN bytes >= 1024 THEN concat(round(bytes/1024,2),' KB') ELSE concat(bytes,' B') END"),
        ("format_duration", "seconds INT", "STRING", "Formats seconds to HH:MM:SS", "concat(lpad(cast(floor(seconds/3600) as STRING),2,'0'),':',lpad(cast(floor((seconds%3600)/60) as STRING),2,'0'),':',lpad(cast(seconds%60 as STRING),2,'0'))"),
        ("call_cost_estimate", "duration_seconds INT, rate_per_min DECIMAL(6,4)", "DECIMAL(8,4)", "Estimates call cost", "round(duration_seconds / 60.0 * rate_per_min, 4)"),
        ("churn_risk_label", "probability DOUBLE", "STRING", "Labels churn risk", "CASE WHEN probability >= 0.8 THEN 'Critical' WHEN probability >= 0.6 THEN 'High' WHEN probability >= 0.4 THEN 'Medium' WHEN probability >= 0.2 THEN 'Low' ELSE 'Minimal' END"),
        ("network_generation", "tech STRING", "INT", "Returns network generation number", "CASE WHEN tech LIKE '5G%' THEN 5 WHEN tech LIKE '4G%' THEN 4 WHEN tech LIKE '3G%' THEN 3 ELSE 2 END"),
        ("signal_quality", "strength INT", "STRING", "Signal quality label", "CASE WHEN strength >= 4 THEN 'Excellent' WHEN strength >= 3 THEN 'Good' WHEN strength >= 2 THEN 'Fair' ELSE 'Poor' END"),
        ("is_roaming", "country STRING", "BOOLEAN", "Checks if country is roaming", "country != 'US'"),
        ("billing_status_priority", "status STRING", "INT", "Priority for billing status", "CASE WHEN status = 'overdue' THEN 1 WHEN status = 'partial' THEN 2 WHEN status = 'pending' THEN 3 WHEN status = 'paid' THEN 4 ELSE 5 END"),
        ("subscriber_tenure_months", "activation_date DATE", "INT", "Months since activation", "months_between(current_date(), activation_date)"),
        ("data_plan_usage_pct", "used_gb DOUBLE, limit_gb INT", "DECIMAL(5,2)", "Plan usage percentage", "CASE WHEN limit_gb = -1 THEN 0 ELSE round(used_gb / limit_gb * 100, 2) END"),
        ("device_age_months", "activation_date DATE", "INT", "Device age in months", "months_between(current_date(), activation_date)"),
        ("is_contract_ending", "end_date DATE", "BOOLEAN", "Contract ending within 60 days", "datediff(end_date, current_date()) BETWEEN 0 AND 60"),
        ("tower_distance_km", "lat1 DOUBLE, lon1 DOUBLE, lat2 DOUBLE, lon2 DOUBLE", "DOUBLE", "Haversine distance between coordinates", "round(6371 * acos(cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon2) - radians(lon1)) + sin(radians(lat1)) * sin(radians(lat2))), 2)"),
        ("anonymize_subscriber", "first_name STRING, subscriber_id BIGINT", "STRING", "Anonymized subscriber ref", "concat('SUB-',substring(sha2(concat(first_name,cast(subscriber_id as STRING)),256),1,8))"),
        ("format_phone_display", "phone STRING", "STRING", "Formats phone for display", "concat('(',substring(phone,3,3),') ',substring(phone,6,3),'-',substring(phone,9,4))"),
        ("ticket_sla_status", "created DATE, resolved DATE, priority STRING", "STRING", "SLA compliance check", "CASE WHEN resolved IS NULL THEN 'Open' WHEN datediff(resolved,created) <= CASE WHEN priority='critical' THEN 1 WHEN priority='high' THEN 3 WHEN priority='medium' THEN 5 ELSE 7 END THEN 'Met' ELSE 'Breached' END"),
        ("spectrum_band_category", "frequency_mhz INT", "STRING", "Categorizes spectrum band", "CASE WHEN frequency_mhz < 1000 THEN 'Low Band' WHEN frequency_mhz < 6000 THEN 'Mid Band' ELSE 'High Band (mmWave)' END"),
        ("monthly_arpu", "total DECIMAL(8,2), months INT", "DECIMAL(8,2)", "Average revenue per user per month", "round(total / GREATEST(months, 1), 2)"),
    ],
}

INDUSTRIES["manufacturing"] = {
    "tables": [
        {"name": "sensor_readings", "rows": 100_000_000, "ddl_cols": "reading_id BIGINT, equipment_id BIGINT, sensor_type STRING, value DOUBLE, unit STRING, reading_timestamp TIMESTAMP, quality_flag STRING, line_id BIGINT, shift STRING, alert_level STRING",
         "insert_expr": "id + {offset} AS reading_id, floor(rand()*10000)+1 AS equipment_id, element_at(array('temperature','pressure','vibration','humidity','speed','flow_rate','voltage','current','torque','ph'),cast(floor(rand()*10)+1 as INT)) AS sensor_type, round(rand()*1000,4) AS value, element_at(array('°C','PSI','mm/s','%RH','RPM','L/min','V','A','Nm','pH'),cast(floor(rand()*10)+1 as INT)) AS unit, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS reading_timestamp, element_at(array('normal','warning','critical','calibration_needed'),cast(floor(rand()*4)+1 as INT)) AS quality_flag, floor(rand()*50)+1 AS line_id, element_at(array('morning','afternoon','night'),cast(floor(rand()*3)+1 as INT)) AS shift, element_at(array('none','low','medium','high','critical'),cast(floor(rand()*5)+1 as INT)) AS alert_level"},
        {"name": "production_events", "rows": 50_000_000, "ddl_cols": "event_id BIGINT, order_id BIGINT, product_code STRING, line_id BIGINT, event_type STRING, quantity INT, event_timestamp TIMESTAMP, operator_id BIGINT, batch_id STRING, cycle_time_seconds DOUBLE",
         "insert_expr": "id + {offset} AS event_id, floor(rand()*5000000)+1 AS order_id, concat('PRD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS product_code, floor(rand()*50)+1 AS line_id, element_at(array('start','complete','pause','resume','scrap','rework','changeover','maintenance'),cast(floor(rand()*8)+1 as INT)) AS event_type, cast(floor(rand()*1000)+1 as INT) AS quantity, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp, floor(rand()*500)+1 AS operator_id, concat('BATCH-',upper(substring(sha2(cast(floor(id/100) as STRING),256),1,8))) AS batch_id, round(rand()*120+5,2) AS cycle_time_seconds"},
        {"name": "quality_checks", "rows": 30_000_000, "ddl_cols": "check_id BIGINT, batch_id STRING, product_code STRING, check_type STRING, result STRING, measurement DOUBLE, spec_min DOUBLE, spec_max DOUBLE, inspector_id BIGINT, check_timestamp TIMESTAMP",
         "insert_expr": "id + {offset} AS check_id, concat('BATCH-',upper(substring(sha2(cast(floor(rand()*5000000) as STRING),256),1,8))) AS batch_id, concat('PRD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS product_code, element_at(array('dimensional','visual','functional','chemical','electrical','stress','weight','color','hardness','leak'),cast(floor(rand()*10)+1 as INT)) AS check_type, element_at(array('pass','fail','marginal','retest'),cast(floor(rand()*4)+1 as INT)) AS result, round(rand()*100,4) AS measurement, round(rand()*40,4) AS spec_min, round(rand()*40+60,4) AS spec_max, floor(rand()*200)+1 AS inspector_id, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS check_timestamp"},
        {"name": "work_orders", "rows": 5_000_000, "ddl_cols": "order_id BIGINT, product_code STRING, quantity_ordered INT, quantity_produced INT, quantity_scrapped INT, start_date DATE, due_date DATE, completion_date DATE, status STRING, priority STRING",
         "insert_expr": "id + {offset} AS order_id, concat('PRD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS product_code, cast(floor(rand()*10000)+100 as INT) AS quantity_ordered, cast(floor(rand()*10000)+50 as INT) AS quantity_produced, cast(floor(rand()*500) as INT) AS quantity_scrapped, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS start_date, date_add('2020-01-01',cast(floor(rand()*1825)+14 as INT)) AS due_date, date_add('2020-01-01',cast(floor(rand()*1825)+10 as INT)) AS completion_date, element_at(array('planned','in_progress','completed','on_hold','cancelled'),cast(floor(rand()*5)+1 as INT)) AS status, element_at(array('critical','high','normal','low'),cast(floor(rand()*4)+1 as INT)) AS priority"},
        {"name": "downtime_events", "rows": 5_000_000, "ddl_cols": "downtime_id BIGINT, equipment_id BIGINT, line_id BIGINT, reason STRING, start_time TIMESTAMP, end_time TIMESTAMP, duration_minutes INT, category STRING, planned BOOLEAN, cost_impact DECIMAL(10,2)",
         "insert_expr": "id + {offset} AS downtime_id, floor(rand()*10000)+1 AS equipment_id, floor(rand()*50)+1 AS line_id, element_at(array('mechanical_failure','electrical_fault','material_shortage','changeover','cleaning','calibration','operator_error','software_issue','power_outage','scheduled_maintenance'),cast(floor(rand()*10)+1 as INT)) AS reason, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS start_time, dateadd(SECOND, cast(floor(rand()*1825*86400)+7200 as INT), '2020-01-01') AS end_time, cast(floor(rand()*480)+5 as INT) AS duration_minutes, element_at(array('planned','unplanned','emergency'),cast(floor(rand()*3)+1 as INT)) AS category, rand() > 0.6 AS planned, round(rand()*50000+100,2) AS cost_impact"},
        {"name": "equipment", "rows": 1_000_000, "ddl_cols": "equipment_id BIGINT, name STRING, equipment_type STRING, manufacturer STRING, model STRING, serial_number STRING, install_date DATE, line_id BIGINT, status STRING, maintenance_interval_days INT",
         "insert_expr": "id + {offset} AS equipment_id, concat(element_at(array('CNC','Press','Robot','Conveyor','Mixer','Furnace','Grinder','Laser','Welder','Pump'),cast(floor(rand()*10)+1 as INT)),'-',lpad(cast(id as STRING),4,'0')) AS name, element_at(array('cnc_machine','hydraulic_press','robotic_arm','conveyor','mixer','furnace','grinder','laser_cutter','welder','pump'),cast(floor(rand()*10)+1 as INT)) AS equipment_type, element_at(array('Siemens','ABB','Fanuc','Bosch','Mitsubishi','Haas','DMG Mori','Trumpf','Lincoln','Caterpillar'),cast(floor(rand()*10)+1 as INT)) AS manufacturer, concat('Model-',upper(substring(sha2(cast(id as STRING),256),1,6))) AS model, concat('SN-',upper(substring(sha2(cast(id+1000 as STRING),256),1,10))) AS serial_number, date_add('2010-01-01',cast(floor(rand()*5475) as INT)) AS install_date, floor(rand()*50)+1 AS line_id, element_at(array('running','idle','maintenance','offline','decommissioned'),cast(floor(rand()*5)+1 as INT)) AS status, element_at(array(30,60,90,180,365),cast(floor(rand()*5)+1 as INT)) AS maintenance_interval_days"},
        {"name": "materials", "rows": 1_000_000, "ddl_cols": "material_id BIGINT, name STRING, material_type STRING, unit STRING, unit_cost DECIMAL(10,4), supplier_id BIGINT, lead_time_days INT, min_order_qty INT, safety_stock INT",
         "insert_expr": "id + {offset} AS material_id, concat(element_at(array('Steel','Aluminum','Copper','Plastic','Rubber','Glass','Carbon Fiber','Titanium','Ceramic','Silicon'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Sheet','Rod','Wire','Pellet','Powder','Plate','Tube','Bar','Coil','Film'),cast(floor(rand()*10)+1 as INT))) AS name, element_at(array('raw','component','packaging','consumable','chemical'),cast(floor(rand()*5)+1 as INT)) AS material_type, element_at(array('kg','m','pcs','L','m²','rolls','sheets'),cast(floor(rand()*7)+1 as INT)) AS unit, round(rand()*500+0.01,4) AS unit_cost, floor(rand()*1000)+1 AS supplier_id, cast(floor(rand()*60)+1 as INT) AS lead_time_days, cast(floor(rand()*1000)+10 as INT) AS min_order_qty, cast(floor(rand()*5000)+100 as INT) AS safety_stock"},
        {"name": "suppliers_mfg", "rows": 1_000_000, "ddl_cols": "supplier_id BIGINT, name STRING, country STRING, certification STRING, quality_rating DECIMAL(3,1), on_time_delivery_pct DECIMAL(5,2), contact_email STRING, lead_time_avg_days INT",
         "insert_expr": "id + {offset} AS supplier_id, concat(element_at(array('Global','Pacific','Industrial','Premier','Quality','Precision','Allied','National','Advanced','Superior'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Materials','Components','Supply','Parts','Manufacturing','Metals','Plastics','Chemicals'),cast(floor(rand()*8)+1 as INT)),' Inc') AS name, element_at(array('US','CN','DE','JP','KR','TW','MX','IN','IT','UK'),cast(floor(rand()*10)+1 as INT)) AS country, element_at(array('ISO 9001','ISO 14001','IATF 16949','AS9100','ISO 13485','None'),cast(floor(rand()*6)+1 as INT)) AS certification, round(rand()*4+1,1) AS quality_rating, round(rand()*30+70,2) AS on_time_delivery_pct, concat('supplier',id + {offset},'@example.com') AS contact_email, cast(floor(rand()*60)+3 as INT) AS lead_time_avg_days"},
        {"name": "production_lines", "rows": 1_000_000, "ddl_cols": "line_id BIGINT, name STRING, plant STRING, capacity_per_hour INT, product_family STRING, shift_pattern STRING, status STRING, oee_target DECIMAL(5,2)",
         "insert_expr": "id + {offset} AS line_id, concat('Line-',element_at(array('A','B','C','D','E','F','G','H','J','K'),cast(floor(rand()*10)+1 as INT)),'-',lpad(cast(id as STRING),3,'0')) AS name, element_at(array('Plant North','Plant South','Plant East','Plant West','Main Plant'),cast(floor(rand()*5)+1 as INT)) AS plant, cast(floor(rand()*500)+50 as INT) AS capacity_per_hour, element_at(array('automotive','electronics','consumer','industrial','aerospace','medical'),cast(floor(rand()*6)+1 as INT)) AS product_family, element_at(array('3x8','2x12','1x8','24/7'),cast(floor(rand()*4)+1 as INT)) AS shift_pattern, element_at(array('running','changeover','maintenance','idle'),cast(floor(rand()*4)+1 as INT)) AS status, round(rand()*20+75,2) AS oee_target"},
        {"name": "products_mfg", "rows": 1_000_000, "ddl_cols": "product_code STRING, name STRING, category STRING, weight_kg DECIMAL(8,3), dimensions STRING, bom_cost DECIMAL(10,2), selling_price DECIMAL(10,2), active BOOLEAN",
         "insert_expr": "concat('PRD-',lpad(cast(id + {offset} as STRING),5,'0')) AS product_code, concat(element_at(array('Heavy Duty','Precision','Standard','Custom','Premium','Industrial','Compact','Ultra','Micro','Mega'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Bearing','Shaft','Gear','Valve','Bracket','Housing','Connector','Sensor','Motor','Assembly'),cast(floor(rand()*10)+1 as INT))) AS name, element_at(array('automotive','electronics','consumer','industrial','aerospace','medical'),cast(floor(rand()*6)+1 as INT)) AS category, round(rand()*100+0.01,3) AS weight_kg, concat(cast(floor(rand()*100)+1 as STRING),'x',cast(floor(rand()*100)+1 as STRING),'x',cast(floor(rand()*100)+1 as STRING),'mm') AS dimensions, round(rand()*500+5,2) AS bom_cost, round(rand()*1000+10,2) AS selling_price, rand() > 0.1 AS active"},
        {"name": "maintenance_logs", "rows": 500_000, "ddl_cols": "log_id BIGINT, equipment_id BIGINT, maintenance_type STRING, description STRING, start_time TIMESTAMP, end_time TIMESTAMP, cost DECIMAL(10,2), technician STRING, parts_used STRING",
         "insert_expr": "id + {offset} AS log_id, floor(rand()*10000)+1 AS equipment_id, element_at(array('preventive','corrective','predictive','emergency','calibration'),cast(floor(rand()*5)+1 as INT)) AS maintenance_type, concat('Maintenance on equipment ',floor(rand()*10000)+1) AS description, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS start_time, dateadd(SECOND, cast(floor(rand()*1825*86400)+14400 as INT), '2020-01-01') AS end_time, round(rand()*10000+50,2) AS cost, concat('Tech-',floor(rand()*200)+1) AS technician, element_at(array('bearings','filters','belts','seals','motors','sensors','valves','pumps'),cast(floor(rand()*8)+1 as INT)) AS parts_used"},
        {"name": "scrap_records", "rows": 500_000, "ddl_cols": "scrap_id BIGINT, batch_id STRING, product_code STRING, quantity INT, reason STRING, scrap_date DATE, cost DECIMAL(10,2), disposition STRING",
         "insert_expr": "id + {offset} AS scrap_id, concat('BATCH-',upper(substring(sha2(cast(floor(rand()*5000000) as STRING),256),1,8))) AS batch_id, concat('PRD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS product_code, cast(floor(rand()*100)+1 as INT) AS quantity, element_at(array('dimensional','surface_defect','material_defect','assembly_error','contamination','equipment_malfunction'),cast(floor(rand()*6)+1 as INT)) AS reason, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS scrap_date, round(rand()*5000+10,2) AS cost, element_at(array('recycled','scrapped','reworked','returned_to_supplier'),cast(floor(rand()*4)+1 as INT)) AS disposition"},
        {"name": "energy_consumption", "rows": 300_000, "ddl_cols": "record_id BIGINT, line_id BIGINT, meter_type STRING, value DOUBLE, unit STRING, reading_date DATE, shift STRING, cost DECIMAL(8,2)",
         "insert_expr": "id + {offset} AS record_id, floor(rand()*50)+1 AS line_id, element_at(array('electricity','natural_gas','water','compressed_air','steam'),cast(floor(rand()*5)+1 as INT)) AS meter_type, round(rand()*10000,2) AS value, element_at(array('kWh','therms','gallons','m³','lbs'),cast(floor(rand()*5)+1 as INT)) AS unit, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS reading_date, element_at(array('morning','afternoon','night'),cast(floor(rand()*3)+1 as INT)) AS shift, round(rand()*5000+10,2) AS cost"},
        {"name": "safety_incidents", "rows": 200_000, "ddl_cols": "incident_id BIGINT, location STRING, incident_type STRING, severity STRING, incident_date DATE, description STRING, lost_time_hours DECIMAL(6,1), osha_recordable BOOLEAN",
         "insert_expr": "id + {offset} AS incident_id, element_at(array('Plant North','Plant South','Plant East','Plant West','Warehouse','Loading Dock'),cast(floor(rand()*6)+1 as INT)) AS location, element_at(array('slip_trip_fall','struck_by','caught_in','electrical','chemical','ergonomic','fire','near_miss'),cast(floor(rand()*8)+1 as INT)) AS incident_type, element_at(array('minor','moderate','serious','critical'),cast(floor(rand()*4)+1 as INT)) AS severity, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS incident_date, concat('Safety incident at ',element_at(array('assembly','welding','painting','machining','warehouse'),cast(floor(rand()*5)+1 as INT)),' area') AS description, round(rand()*40,1) AS lost_time_hours, rand() > 0.7 AS osha_recordable"},
        {"name": "inventory_mfg", "rows": 300_000, "ddl_cols": "inventory_id BIGINT, material_id BIGINT, warehouse STRING, quantity DECIMAL(12,2), unit STRING, last_counted DATE, min_level DECIMAL(12,2), max_level DECIMAL(12,2)",
         "insert_expr": "id + {offset} AS inventory_id, floor(rand()*1000000)+1 AS material_id, element_at(array('Raw Materials','WIP','Finished Goods','MRO','Receiving'),cast(floor(rand()*5)+1 as INT)) AS warehouse, round(rand()*100000,2) AS quantity, element_at(array('kg','m','pcs','L'),cast(floor(rand()*4)+1 as INT)) AS unit, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS last_counted, round(rand()*1000+100,2) AS min_level, round(rand()*100000+10000,2) AS max_level"},
        {"name": "purchase_orders", "rows": 200_000, "ddl_cols": "po_id BIGINT, supplier_id BIGINT, material_id BIGINT, quantity DECIMAL(12,2), unit_price DECIMAL(10,4), total DECIMAL(14,2), order_date DATE, delivery_date DATE, status STRING",
         "insert_expr": "id + {offset} AS po_id, floor(rand()*1000000)+1 AS supplier_id, floor(rand()*1000000)+1 AS material_id, round(rand()*10000+10,2) AS quantity, round(rand()*500+0.01,4) AS unit_price, round(rand()*500000+10,2) AS total, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS order_date, date_add('2020-01-01',cast(floor(rand()*1825)+14 as INT)) AS delivery_date, element_at(array('ordered','shipped','received','inspected','cancelled'),cast(floor(rand()*5)+1 as INT)) AS status"},
        {"name": "bom", "rows": 200_000, "ddl_cols": "bom_id BIGINT, product_code STRING, material_id BIGINT, quantity_per_unit DECIMAL(8,4), unit STRING, level INT, is_critical BOOLEAN",
         "insert_expr": "id + {offset} AS bom_id, concat('PRD-',lpad(cast(floor(rand()*99999) as STRING),5,'0')) AS product_code, floor(rand()*1000000)+1 AS material_id, round(rand()*10+0.01,4) AS quantity_per_unit, element_at(array('pcs','kg','m','L','sheets'),cast(floor(rand()*5)+1 as INT)) AS unit, cast(floor(rand()*5)+1 as INT) AS level, rand() > 0.7 AS is_critical"},
        {"name": "shift_schedules", "rows": 100_000, "ddl_cols": "schedule_id BIGINT, operator_id BIGINT, line_id BIGINT, shift_date DATE, shift STRING, start_time STRING, end_time STRING, status STRING",
         "insert_expr": "id + {offset} AS schedule_id, floor(rand()*500)+1 AS operator_id, floor(rand()*50)+1 AS line_id, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS shift_date, element_at(array('morning','afternoon','night'),cast(floor(rand()*3)+1 as INT)) AS shift, element_at(array('06:00','14:00','22:00'),cast(floor(rand()*3)+1 as INT)) AS start_time, element_at(array('14:00','22:00','06:00'),cast(floor(rand()*3)+1 as INT)) AS end_time, element_at(array('scheduled','worked','absent','overtime'),cast(floor(rand()*4)+1 as INT)) AS status"},
        {"name": "environmental_metrics", "rows": 100_000, "ddl_cols": "metric_id BIGINT, plant STRING, metric_type STRING, value DOUBLE, unit STRING, measurement_date DATE, compliant BOOLEAN",
         "insert_expr": "id + {offset} AS metric_id, element_at(array('Plant North','Plant South','Plant East','Plant West','Main Plant'),cast(floor(rand()*5)+1 as INT)) AS plant, element_at(array('co2_emissions','water_usage','waste_generated','energy_intensity','recycling_rate','voc_emissions'),cast(floor(rand()*6)+1 as INT)) AS metric_type, round(rand()*1000,2) AS value, element_at(array('tons','gallons','kg','kWh/unit','%','ppm'),cast(floor(rand()*6)+1 as INT)) AS unit, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS measurement_date, rand() > 0.15 AS compliant"},
    ],
    "views": [
        ("v_oee_by_line", "SELECT pl.line_id, pl.name, count(DISTINCT pe.order_id) AS orders, sum(pe.quantity) AS total_produced, avg(pe.cycle_time_seconds) AS avg_cycle_time FROM {s}.production_lines pl JOIN {s}.production_events pe ON pl.line_id = pe.line_id WHERE pe.event_type = 'complete' GROUP BY pl.line_id, pl.name"),
        ("v_quality_summary", "SELECT product_code, check_type, count(*) AS checks, sum(CASE WHEN result='pass' THEN 1 ELSE 0 END) AS passed, round(sum(CASE WHEN result='pass' THEN 1 ELSE 0 END)*100.0/count(*),2) AS pass_rate FROM {s}.quality_checks GROUP BY product_code, check_type"),
        ("v_equipment_health", "SELECT e.equipment_id, e.name, e.equipment_type, e.status, count(dt.downtime_id) AS downtime_events, sum(dt.duration_minutes) AS total_downtime_min FROM {s}.equipment e LEFT JOIN {s}.downtime_events dt ON e.equipment_id = dt.equipment_id GROUP BY e.equipment_id, e.name, e.equipment_type, e.status"),
        ("v_sensor_alerts", "SELECT sr.equipment_id, sr.sensor_type, sr.alert_level, count(*) AS alerts, avg(sr.value) AS avg_value FROM {s}.sensor_readings sr WHERE sr.alert_level IN ('high','critical') GROUP BY sr.equipment_id, sr.sensor_type, sr.alert_level"),
        ("v_work_order_status", "SELECT status, priority, count(*) AS orders, sum(quantity_ordered) AS total_ordered, sum(quantity_produced) AS total_produced, sum(quantity_scrapped) AS total_scrapped FROM {s}.work_orders GROUP BY status, priority"),
        ("v_scrap_analysis", "SELECT reason, count(*) AS incidents, sum(quantity) AS total_scrapped, sum(cost) AS total_cost FROM {s}.scrap_records GROUP BY reason ORDER BY total_cost DESC"),
        ("v_maintenance_schedule", "SELECT e.equipment_id, e.name, e.maintenance_interval_days, max(ml.start_time) AS last_maintenance, datediff(current_date(), cast(max(ml.start_time) as DATE)) AS days_since_maintenance FROM {s}.equipment e LEFT JOIN {s}.maintenance_logs ml ON e.equipment_id = ml.equipment_id GROUP BY e.equipment_id, e.name, e.maintenance_interval_days"),
        ("v_supplier_performance", "SELECT s.name, s.country, s.quality_rating, s.on_time_delivery_pct, count(po.po_id) AS total_orders FROM {s}.suppliers_mfg s LEFT JOIN {s}.purchase_orders po ON s.supplier_id = po.supplier_id GROUP BY s.name, s.country, s.quality_rating, s.on_time_delivery_pct"),
        ("v_energy_by_line", "SELECT line_id, meter_type, date_trunc('month',reading_date) AS month, sum(value) AS total_consumption, sum(cost) AS total_cost FROM {s}.energy_consumption GROUP BY line_id, meter_type, date_trunc('month',reading_date)"),
        ("v_safety_dashboard", "SELECT date_trunc('month',incident_date) AS month, incident_type, severity, count(*) AS incidents, sum(lost_time_hours) AS total_lost_hours FROM {s}.safety_incidents GROUP BY date_trunc('month',incident_date), incident_type, severity"),
        ("v_production_daily", "SELECT cast(event_timestamp as DATE) AS production_date, line_id, count(*) AS events, sum(CASE WHEN event_type='complete' THEN quantity ELSE 0 END) AS completed FROM {s}.production_events GROUP BY cast(event_timestamp as DATE), line_id"),
        ("v_material_usage", "SELECT m.name, m.material_type, sum(b.quantity_per_unit * wo.quantity_produced) AS total_used FROM {s}.materials m JOIN {s}.bom b ON m.material_id = b.material_id JOIN {s}.work_orders wo ON b.product_code = wo.product_code GROUP BY m.name, m.material_type"),
        ("v_inventory_status", "SELECT i.warehouse, count(*) AS items, sum(CASE WHEN i.quantity < i.min_level THEN 1 ELSE 0 END) AS below_min, sum(CASE WHEN i.quantity > i.max_level THEN 1 ELSE 0 END) AS above_max FROM {s}.inventory_mfg i GROUP BY i.warehouse"),
        ("v_downtime_pareto", "SELECT reason, count(*) AS occurrences, sum(duration_minutes) AS total_minutes, sum(cost_impact) AS total_cost FROM {s}.downtime_events WHERE planned = false GROUP BY reason ORDER BY total_minutes DESC"),
        ("v_product_profitability", "SELECT p.product_code, p.name, p.selling_price, p.bom_cost, round((p.selling_price - p.bom_cost)/p.selling_price*100,2) AS margin_pct FROM {s}.products_mfg p WHERE p.active = true"),
        ("v_po_delivery", "SELECT s.name AS supplier, count(po.po_id) AS orders, avg(datediff(po.delivery_date,po.order_date)) AS avg_lead_days FROM {s}.purchase_orders po JOIN {s}.suppliers_mfg s ON po.supplier_id = s.supplier_id GROUP BY s.name"),
        ("v_shift_productivity", "SELECT ss.shift, ss.line_id, count(DISTINCT ss.operator_id) AS operators, count(DISTINCT pe.event_id) AS events FROM {s}.shift_schedules ss LEFT JOIN {s}.production_events pe ON ss.line_id = pe.line_id AND ss.shift = pe.shift GROUP BY ss.shift, ss.line_id"),
        ("v_environmental_compliance", "SELECT plant, metric_type, count(*) AS measurements, sum(CASE WHEN compliant THEN 1 ELSE 0 END) AS compliant_count, round(sum(CASE WHEN compliant THEN 1 ELSE 0 END)*100.0/count(*),2) AS compliance_pct FROM {s}.environmental_metrics GROUP BY plant, metric_type"),
        ("v_critical_bom_items", "SELECT b.product_code, m.name AS material, m.material_type, b.quantity_per_unit, m.unit_cost, s.on_time_delivery_pct FROM {s}.bom b JOIN {s}.materials m ON b.material_id = m.material_id JOIN {s}.suppliers_mfg s ON m.supplier_id = s.supplier_id WHERE b.is_critical = true"),
        ("v_equipment_lifecycle", "SELECT equipment_type, avg(datediff(current_date(),install_date)/365) AS avg_age_years, count(*) AS total, sum(CASE WHEN status='decommissioned' THEN 1 ELSE 0 END) AS decommissioned FROM {s}.equipment GROUP BY equipment_type"),
    ],
    "udfs": [
        ("oee_calculate", "availability DOUBLE, performance DOUBLE, quality DOUBLE", "DOUBLE", "Calculates Overall Equipment Effectiveness", "round(availability * performance * quality / 10000, 2)"),
        ("sensor_status", "value DOUBLE, min_val DOUBLE, max_val DOUBLE", "STRING", "Sensor reading status", "CASE WHEN value < min_val OR value > max_val THEN 'Out of Spec' WHEN value < min_val*1.1 OR value > max_val*0.9 THEN 'Warning' ELSE 'Normal' END"),
        ("cycle_time_efficiency", "actual DOUBLE, target DOUBLE", "DECIMAL(5,2)", "Cycle time efficiency percentage", "round(target / actual * 100, 2)"),
        ("scrap_rate", "scrapped INT, produced INT", "DECIMAL(5,2)", "Scrap rate percentage", "round(scrapped * 100.0 / GREATEST(produced, 1), 2)"),
        ("yield_rate", "good INT, total INT", "DECIMAL(5,2)", "First pass yield", "round(good * 100.0 / GREATEST(total, 1), 2)"),
        ("maintenance_urgency", "days_since INT, interval_days INT", "STRING", "Maintenance urgency level", "CASE WHEN days_since > interval_days * 1.5 THEN 'Overdue - Critical' WHEN days_since > interval_days THEN 'Overdue' WHEN days_since > interval_days * 0.9 THEN 'Due Soon' ELSE 'OK' END"),
        ("downtime_cost_per_hour", "cost DECIMAL(10,2), minutes INT", "DECIMAL(10,2)", "Cost per hour of downtime", "round(cost / GREATEST(minutes, 1) * 60, 2)"),
        ("format_duration_mfg", "minutes INT", "STRING", "Formats duration from minutes", "concat(cast(floor(minutes/60) as STRING),'h ',cast(minutes%60 as STRING),'m')"),
        ("safety_severity_score", "severity STRING, lost_hours DECIMAL(6,1)", "INT", "Numeric safety severity score", "CASE WHEN severity = 'critical' THEN 100 WHEN severity = 'serious' THEN 75 WHEN severity = 'moderate' THEN 50 ELSE 25 END + cast(lost_hours as INT)"),
        ("inventory_health", "quantity DECIMAL(12,2), min_level DECIMAL(12,2), max_level DECIMAL(12,2)", "STRING", "Inventory health status", "CASE WHEN quantity < min_level THEN 'Critical Low' WHEN quantity < min_level * 1.5 THEN 'Low' WHEN quantity > max_level THEN 'Overstocked' ELSE 'Healthy' END"),
        ("energy_cost_per_unit", "energy_cost DECIMAL(8,2), units_produced INT", "DECIMAL(8,4)", "Energy cost per unit produced", "round(energy_cost / GREATEST(units_produced, 1), 4)"),
        ("supplier_grade", "quality DECIMAL(3,1), delivery DECIMAL(5,2)", "STRING", "Supplier grade from metrics", "CASE WHEN quality >= 4.5 AND delivery >= 95 THEN 'A' WHEN quality >= 3.5 AND delivery >= 85 THEN 'B' WHEN quality >= 2.5 AND delivery >= 75 THEN 'C' ELSE 'D' END"),
        ("batch_traceability", "batch_id STRING", "STRING", "Extracts trace info from batch ID", "concat('Batch: ',batch_id,' | Generated: ',current_date())"),
        ("is_overdue_po", "delivery_date DATE", "BOOLEAN", "Checks if PO is overdue", "delivery_date < current_date()"),
        ("shift_label", "shift STRING", "STRING", "Human-readable shift label", "CASE WHEN shift = 'morning' THEN '1st Shift (6AM-2PM)' WHEN shift = 'afternoon' THEN '2nd Shift (2PM-10PM)' ELSE '3rd Shift (10PM-6AM)' END"),
        ("environmental_rating", "compliance_pct DECIMAL(5,2)", "STRING", "Environmental compliance rating", "CASE WHEN compliance_pct >= 98 THEN 'Excellent' WHEN compliance_pct >= 90 THEN 'Good' WHEN compliance_pct >= 80 THEN 'Acceptable' ELSE 'Needs Improvement' END"),
        ("mask_serial", "serial STRING", "STRING", "Masks equipment serial number", "concat('SN-***',right(serial,4))"),
        ("production_status", "produced INT, ordered INT", "STRING", "Production completion status", "CASE WHEN produced >= ordered THEN 'Complete' WHEN produced >= ordered * 0.75 THEN 'Nearly Complete' WHEN produced >= ordered * 0.5 THEN 'In Progress' ELSE 'Early Stage' END"),
        ("quality_cpk", "measurement DOUBLE, spec_min DOUBLE, spec_max DOUBLE", "DOUBLE", "Simplified process capability", "round(LEAST(measurement - spec_min, spec_max - measurement) / ((spec_max - spec_min) / 6), 3)"),
        ("anonymize_operator", "operator_id BIGINT", "STRING", "Anonymized operator reference", "concat('OP-',substring(sha2(cast(operator_id as STRING),256),1,8))"),
    ],
}

# --- Additional Industries (compact definitions, 20 tables each) ---

def _quick_table(name, rows, cols, expr):
    """Shorthand for table definition."""
    return {"name": name, "rows": rows, "ddl_cols": cols, "insert_expr": expr}


INDUSTRIES["energy"] = {
    "tables": [
        _quick_table("meter_readings", 100_000_000, "reading_id BIGINT, meter_id BIGINT, reading_type STRING, value DOUBLE, unit STRING, reading_timestamp TIMESTAMP, quality STRING, tariff_code STRING, is_estimated BOOLEAN, location_id BIGINT",
            "id + {offset} AS reading_id, floor(rand()*500000)+1 AS meter_id, element_at(array('electric','gas','water','solar','wind'),cast(floor(rand()*5)+1 as INT)) AS reading_type, round(rand()*1000,4) AS value, element_at(array('kWh','therms','gallons','MWh','m³'),cast(floor(rand()*5)+1 as INT)) AS unit, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS reading_timestamp, element_at(array('actual','estimated','validated'),cast(floor(rand()*3)+1 as INT)) AS quality, concat('T-',lpad(cast(floor(rand()*20) as STRING),2,'0')) AS tariff_code, rand() > 0.9 AS is_estimated, floor(rand()*100000)+1 AS location_id"),
        _quick_table("grid_events", 50_000_000, "event_id BIGINT, station_id BIGINT, event_type STRING, severity STRING, duration_minutes INT, affected_customers INT, event_timestamp TIMESTAMP, cause STRING, restoration_time TIMESTAMP, outage_mw DOUBLE",
            "id + {offset} AS event_id, floor(rand()*5000)+1 AS station_id, element_at(array('outage','voltage_sag','frequency_deviation','overload','maintenance','switching'),cast(floor(rand()*6)+1 as INT)) AS event_type, element_at(array('critical','major','minor','warning'),cast(floor(rand()*4)+1 as INT)) AS severity, cast(floor(rand()*480)+5 as INT) AS duration_minutes, cast(floor(rand()*50000) as INT) AS affected_customers, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp, element_at(array('weather','equipment','vegetation','animal','overload','unknown'),cast(floor(rand()*6)+1 as INT)) AS cause, dateadd(SECOND, cast(floor(rand()*1825*86400)+28800 as INT), '2020-01-01') AS restoration_time, round(rand()*500,2) AS outage_mw"),
        _quick_table("generation_output", 30_000_000, "output_id BIGINT, plant_id BIGINT, fuel_type STRING, output_mwh DOUBLE, capacity_factor DECIMAL(5,2), co2_tons DOUBLE, generation_date DATE, price_per_mwh DECIMAL(8,2), dispatch_order INT, availability DECIMAL(5,2)",
            "id + {offset} AS output_id, floor(rand()*1000)+1 AS plant_id, element_at(array('solar','wind','natural_gas','coal','nuclear','hydro','biomass','geothermal'),cast(floor(rand()*8)+1 as INT)) AS fuel_type, round(rand()*5000,2) AS output_mwh, round(rand()*100,2) AS capacity_factor, round(rand()*2000,2) AS co2_tons, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS generation_date, round(rand()*200+10,2) AS price_per_mwh, cast(floor(rand()*100)+1 as INT) AS dispatch_order, round(rand()*100,2) AS availability"),
        _quick_table("billing_energy", 5_000_000, "bill_id BIGINT, account_id BIGINT, billing_period DATE, usage_kwh DOUBLE, demand_kw DOUBLE, base_charge DECIMAL(8,2), usage_charge DECIMAL(8,2), demand_charge DECIMAL(8,2), taxes DECIMAL(8,2), total DECIMAL(10,2)",
            "id + {offset} AS bill_id, floor(rand()*500000)+1 AS account_id, date_add('2020-01-01',cast(floor(rand()*60)*30 as INT)) AS billing_period, round(rand()*5000+100,2) AS usage_kwh, round(rand()*200+10,2) AS demand_kw, round(rand()*20+5,2) AS base_charge, round(rand()*500+10,2) AS usage_charge, round(rand()*200,2) AS demand_charge, round(rand()*80+5,2) AS taxes, round(rand()*800+20,2) AS total"),
        _quick_table("demand_forecast", 5_000_000, "forecast_id BIGINT, region STRING, forecast_date DATE, hour INT, predicted_mw DOUBLE, actual_mw DOUBLE, temperature_f DOUBLE, model_version STRING, error_pct DECIMAL(5,2), confidence DECIMAL(5,2)",
            "id + {offset} AS forecast_id, element_at(array('Northeast','Southeast','Midwest','Southwest','West','Pacific'),cast(floor(rand()*6)+1 as INT)) AS region, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS forecast_date, cast(floor(rand()*24) as INT) AS hour, round(rand()*50000+5000,2) AS predicted_mw, round(rand()*50000+5000,2) AS actual_mw, round(rand()*60+20,1) AS temperature_f, element_at(array('v1.0','v2.0','v3.0'),cast(floor(rand()*3)+1 as INT)) AS model_version, round(rand()*20-10,2) AS error_pct, round(rand()*30+70,2) AS confidence"),
        _quick_table("customers_energy", 1_000_000, "customer_id BIGINT, name STRING, customer_type STRING, address STRING, city STRING, state STRING, zip STRING, rate_class STRING, meter_id BIGINT, enrolled_date DATE",
            "id + {offset} AS customer_id, concat(element_at(array('James','Mary','John','Patricia','Robert'),cast(floor(rand()*5)+1 as INT)),' ',element_at(array('Smith','Johnson','Williams','Brown','Jones'),cast(floor(rand()*5)+1 as INT))) AS name, element_at(array('residential','commercial','industrial','government'),cast(floor(rand()*4)+1 as INT)) AS customer_type, concat(cast(floor(rand()*9999)+1 as STRING),' Energy Dr') AS address, element_at(array('Houston','Dallas','Phoenix','Denver','LA'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('TX','TX','AZ','CO','CA'),cast(floor(rand()*5)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip, element_at(array('R-1','R-2','C-1','C-2','I-1','I-2'),cast(floor(rand()*6)+1 as INT)) AS rate_class, floor(rand()*500000)+1 AS meter_id, date_add('2010-01-01',cast(floor(rand()*5475) as INT)) AS enrolled_date"),
        _quick_table("power_plants", 1_000_000, "plant_id BIGINT, name STRING, fuel_type STRING, capacity_mw DOUBLE, location STRING, state STRING, commissioned DATE, status STRING, operator STRING, emission_rate DOUBLE",
            "id + {offset} AS plant_id, concat(element_at(array('Sunrise','Thunder','Eagle','River','Mountain','Valley','Prairie','Desert','Coastal','Lake'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Solar Farm','Wind Park','Gas Plant','Station','Power Center'),cast(floor(rand()*5)+1 as INT))) AS name, element_at(array('solar','wind','natural_gas','coal','nuclear','hydro'),cast(floor(rand()*6)+1 as INT)) AS fuel_type, round(rand()*2000+10,2) AS capacity_mw, concat(round(rand()*50+25,4),',',round(rand()*60-120,4)) AS location, element_at(array('TX','CA','FL','NY','PA','IL','OH','GA'),cast(floor(rand()*8)+1 as INT)) AS state, date_add('1980-01-01',cast(floor(rand()*16425) as INT)) AS commissioned, element_at(array('operating','standby','retired','under_construction'),cast(floor(rand()*4)+1 as INT)) AS status, concat('Operator-',floor(rand()*100)+1) AS operator, round(rand()*2,4) AS emission_rate"),
        _quick_table("renewable_certificates", 1_000_000, "cert_id BIGINT, plant_id BIGINT, cert_type STRING, mwh_generated DOUBLE, vintage_year INT, state STRING, status STRING, price DECIMAL(8,2), buyer_id BIGINT, created_date DATE",
            "id + {offset} AS cert_id, floor(rand()*1000)+1 AS plant_id, element_at(array('REC','SREC','I-REC','GO'),cast(floor(rand()*4)+1 as INT)) AS cert_type, round(rand()*1000+1,2) AS mwh_generated, cast(floor(rand()*5)+2020 as INT) AS vintage_year, element_at(array('TX','CA','NY','NJ','MA'),cast(floor(rand()*5)+1 as INT)) AS state, element_at(array('active','retired','traded','expired'),cast(floor(rand()*4)+1 as INT)) AS status, round(rand()*50+1,2) AS price, floor(rand()*100000)+1 AS buyer_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS created_date"),
        _quick_table("substations", 500_000, "station_id BIGINT, name STRING, voltage_kv INT, station_type STRING, latitude DOUBLE, longitude DOUBLE, capacity_mva DOUBLE, status STRING, last_inspection DATE, transformer_count INT",
            "id + {offset} AS station_id, concat('Substation-',lpad(cast(id + {offset} as STRING),5,'0')) AS name, element_at(array(69,115,138,230,345,500,765),cast(floor(rand()*7)+1 as INT)) AS voltage_kv, element_at(array('transmission','distribution','switching','generation'),cast(floor(rand()*4)+1 as INT)) AS station_type, round(rand()*25+25,6) AS latitude, round(rand()*55-125,6) AS longitude, round(rand()*500+10,2) AS capacity_mva, element_at(array('energized','de_energized','maintenance'),cast(floor(rand()*3)+1 as INT)) AS status, date_add('2023-01-01',cast(floor(rand()*730) as INT)) AS last_inspection, cast(floor(rand()*10)+1 as INT) AS transformer_count"),
        _quick_table("tariff_rates", 500_000, "rate_id BIGINT, rate_class STRING, rate_type STRING, price_per_kwh DECIMAL(8,6), effective_date DATE, season STRING, time_of_use STRING, tier INT",
            "id + {offset} AS rate_id, element_at(array('R-1','R-2','C-1','C-2','I-1','I-2','AG-1'),cast(floor(rand()*7)+1 as INT)) AS rate_class, element_at(array('energy','demand','fixed','time_of_use'),cast(floor(rand()*4)+1 as INT)) AS rate_type, round(rand()*0.30+0.02,6) AS price_per_kwh, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date, element_at(array('summer','winter','shoulder'),cast(floor(rand()*3)+1 as INT)) AS season, element_at(array('peak','off_peak','mid_peak','super_off_peak'),cast(floor(rand()*4)+1 as INT)) AS time_of_use, cast(floor(rand()*4)+1 as INT) AS tier"),
        _quick_table("outage_reports", 300_000, "report_id BIGINT, station_id BIGINT, reported_by STRING, report_time TIMESTAMP, cause STRING, customers_affected INT, estimated_restoration TIMESTAMP, actual_restoration TIMESTAMP, status STRING",
            "id + {offset} AS report_id, floor(rand()*5000)+1 AS station_id, concat('Dispatcher-',floor(rand()*50)+1) AS reported_by, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS report_time, element_at(array('storm','equipment','vehicle','animal','planned','unknown'),cast(floor(rand()*6)+1 as INT)) AS cause, cast(floor(rand()*10000) as INT) AS customers_affected, dateadd(SECOND, cast(floor(rand()*1825*86400)+14400 as INT), '2020-01-01') AS estimated_restoration, dateadd(SECOND, cast(floor(rand()*1825*86400)+10800 as INT), '2020-01-01') AS actual_restoration, element_at(array('reported','confirmed','crew_dispatched','restored'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("emissions", 300_000, "emission_id BIGINT, plant_id BIGINT, pollutant STRING, amount_tons DOUBLE, measurement_date DATE, reporting_period STRING, method STRING, compliant BOOLEAN",
            "id + {offset} AS emission_id, floor(rand()*1000)+1 AS plant_id, element_at(array('CO2','SO2','NOx','PM2.5','PM10','Mercury','Lead','VOC'),cast(floor(rand()*8)+1 as INT)) AS pollutant, round(rand()*10000,2) AS amount_tons, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS measurement_date, element_at(array('Q1','Q2','Q3','Q4','annual'),cast(floor(rand()*5)+1 as INT)) AS reporting_period, element_at(array('CEMS','calculated','estimated'),cast(floor(rand()*3)+1 as INT)) AS method, rand() > 0.1 AS compliant"),
        _quick_table("ev_charging", 200_000, "session_id BIGINT, station_id BIGINT, connector_type STRING, energy_kwh DOUBLE, duration_minutes INT, cost DECIMAL(8,2), start_time TIMESTAMP, vehicle_type STRING, payment_method STRING",
            "id + {offset} AS session_id, floor(rand()*10000)+1 AS station_id, element_at(array('CCS','CHAdeMO','J1772','Tesla','Type2'),cast(floor(rand()*5)+1 as INT)) AS connector_type, round(rand()*80+5,2) AS energy_kwh, cast(floor(rand()*120)+10 as INT) AS duration_minutes, round(rand()*40+2,2) AS cost, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS start_time, element_at(array('sedan','SUV','truck','van','bus'),cast(floor(rand()*5)+1 as INT)) AS vehicle_type, element_at(array('credit_card','app','rfid','subscription'),cast(floor(rand()*4)+1 as INT)) AS payment_method"),
        _quick_table("solar_panels", 200_000, "panel_id BIGINT, installation_id BIGINT, manufacturer STRING, capacity_kw DECIMAL(8,2), efficiency DECIMAL(5,2), install_date DATE, warranty_expiry DATE, degradation_rate DECIMAL(5,4), status STRING",
            "id + {offset} AS panel_id, floor(rand()*50000)+1 AS installation_id, element_at(array('SunPower','First Solar','Canadian Solar','JA Solar','Trina','LONGi','JinkoSolar','Hanwha'),cast(floor(rand()*8)+1 as INT)) AS manufacturer, round(rand()*15+0.5,2) AS capacity_kw, round(rand()*5+18,2) AS efficiency, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS install_date, date_add('2040-01-01',cast(floor(rand()*3650) as INT)) AS warranty_expiry, round(rand()*0.005+0.003,4) AS degradation_rate, element_at(array('active','degraded','offline','replaced'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("battery_storage", 200_000, "battery_id BIGINT, site_id BIGINT, technology STRING, capacity_mwh DOUBLE, power_mw DOUBLE, soc_pct DECIMAL(5,2), cycles INT, install_date DATE, status STRING, degradation_pct DECIMAL(5,2)",
            "id + {offset} AS battery_id, floor(rand()*1000)+1 AS site_id, element_at(array('lithium_ion','flow','solid_state','sodium_ion'),cast(floor(rand()*4)+1 as INT)) AS technology, round(rand()*500+1,2) AS capacity_mwh, round(rand()*200+1,2) AS power_mw, round(rand()*100,2) AS soc_pct, cast(floor(rand()*5000) as INT) AS cycles, date_add('2018-01-01',cast(floor(rand()*2555) as INT)) AS install_date, element_at(array('charging','discharging','standby','maintenance'),cast(floor(rand()*4)+1 as INT)) AS status, round(rand()*20,2) AS degradation_pct"),
        _quick_table("transmission_lines", 100_000, "line_id BIGINT, from_station BIGINT, to_station BIGINT, voltage_kv INT, length_miles DOUBLE, capacity_mw DOUBLE, conductor_type STRING, year_built INT, status STRING",
            "id + {offset} AS line_id, floor(rand()*5000)+1 AS from_station, floor(rand()*5000)+1 AS to_station, element_at(array(69,115,138,230,345,500,765),cast(floor(rand()*7)+1 as INT)) AS voltage_kv, round(rand()*200+1,2) AS length_miles, round(rand()*2000+50,2) AS capacity_mw, element_at(array('ACSR','ACSS','HTLS','XLPE'),cast(floor(rand()*4)+1 as INT)) AS conductor_type, cast(floor(rand()*60)+1960 as INT) AS year_built, element_at(array('in_service','planned','under_construction','retired'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("smart_thermostats", 100_000, "device_id BIGINT, customer_id BIGINT, set_temp_f DOUBLE, actual_temp_f DOUBLE, mode STRING, reading_time TIMESTAMP, energy_saved_kwh DOUBLE, is_dr_enrolled BOOLEAN",
            "id + {offset} AS device_id, floor(rand()*500000)+1 AS customer_id, round(rand()*15+65,1) AS set_temp_f, round(rand()*15+65,1) AS actual_temp_f, element_at(array('heat','cool','auto','off','eco'),cast(floor(rand()*5)+1 as INT)) AS mode, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS reading_time, round(rand()*5,2) AS energy_saved_kwh, rand() > 0.6 AS is_dr_enrolled"),
        _quick_table("demand_response", 100_000, "event_id BIGINT, program STRING, event_date DATE, start_hour INT, end_hour INT, target_reduction_mw DOUBLE, actual_reduction_mw DOUBLE, participants INT, compensation_per_mw DECIMAL(8,2)",
            "id + {offset} AS event_id, element_at(array('residential_ac','commercial_load','industrial_curtailment','ev_managed_charging','battery_dispatch'),cast(floor(rand()*5)+1 as INT)) AS program, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS event_date, cast(floor(rand()*8)+12 as INT) AS start_hour, cast(floor(rand()*4)+16 as INT) AS end_hour, round(rand()*500+10,2) AS target_reduction_mw, round(rand()*500+5,2) AS actual_reduction_mw, cast(floor(rand()*50000)+100 as INT) AS participants, round(rand()*200+20,2) AS compensation_per_mw"),
        _quick_table("carbon_credits", 100_000, "credit_id BIGINT, project_type STRING, vintage_year INT, tons_co2 DOUBLE, price_per_ton DECIMAL(8,2), registry STRING, status STRING, buyer STRING, retirement_date DATE",
            "id + {offset} AS credit_id, element_at(array('reforestation','renewable_energy','methane_capture','direct_air_capture','soil_carbon','avoided_deforestation'),cast(floor(rand()*6)+1 as INT)) AS project_type, cast(floor(rand()*5)+2020 as INT) AS vintage_year, round(rand()*10000+1,2) AS tons_co2, round(rand()*80+5,2) AS price_per_ton, element_at(array('Verra','Gold Standard','ACR','CAR'),cast(floor(rand()*4)+1 as INT)) AS registry, element_at(array('active','retired','pending','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status, concat('Buyer-',floor(rand()*500)+1) AS buyer, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS retirement_date"),
    ],
    "views": [
        ("v_daily_generation", "SELECT generation_date, fuel_type, sum(output_mwh) AS total_mwh, avg(capacity_factor) AS avg_cf FROM {s}.generation_output GROUP BY generation_date, fuel_type"),
        ("v_outage_summary", "SELECT cause, count(*) AS outages, sum(customers_affected) AS total_affected, avg(duration_minutes) AS avg_duration FROM {s}.grid_events WHERE event_type = 'outage' GROUP BY cause"),
        ("v_billing_summary", "SELECT date_trunc('month',billing_period) AS month, count(*) AS bills, sum(total) AS revenue, avg(usage_kwh) AS avg_usage FROM {s}.billing_energy GROUP BY date_trunc('month',billing_period)"),
        ("v_emissions_by_plant", "SELECT pp.name, pp.fuel_type, e.pollutant, sum(e.amount_tons) AS total_tons FROM {s}.emissions e JOIN {s}.power_plants pp ON e.plant_id = pp.plant_id GROUP BY pp.name, pp.fuel_type, e.pollutant"),
        ("v_renewable_mix", "SELECT fuel_type, sum(capacity_mw) AS total_capacity, count(*) AS plants FROM {s}.power_plants WHERE fuel_type IN ('solar','wind','hydro','geothermal','biomass') GROUP BY fuel_type"),
        ("v_ev_charging_trends", "SELECT connector_type, date_trunc('month',start_time) AS month, count(*) AS sessions, sum(energy_kwh) AS total_kwh FROM {s}.ev_charging GROUP BY connector_type, date_trunc('month',start_time)"),
        ("v_forecast_accuracy", "SELECT region, model_version, avg(abs(error_pct)) AS mae, count(*) AS forecasts FROM {s}.demand_forecast GROUP BY region, model_version"),
        ("v_battery_fleet", "SELECT technology, status, count(*) AS units, sum(capacity_mwh) AS total_mwh FROM {s}.battery_storage GROUP BY technology, status"),
        ("v_carbon_portfolio", "SELECT project_type, registry, sum(tons_co2) AS total_tons, avg(price_per_ton) AS avg_price FROM {s}.carbon_credits WHERE status = 'active' GROUP BY project_type, registry"),
        ("v_customer_by_type", "SELECT customer_type, rate_class, count(*) AS customers FROM {s}.customers_energy GROUP BY customer_type, rate_class"),
        ("v_grid_reliability", "SELECT station_id, count(*) AS events, avg(duration_minutes) AS avg_duration FROM {s}.grid_events GROUP BY station_id"),
        ("v_solar_performance", "SELECT manufacturer, avg(efficiency) AS avg_efficiency, count(*) AS panels FROM {s}.solar_panels WHERE status = 'active' GROUP BY manufacturer"),
        ("v_demand_response_performance", "SELECT program, count(*) AS events, sum(actual_reduction_mw) AS total_mw_reduced FROM {s}.demand_response GROUP BY program"),
        ("v_tariff_comparison", "SELECT rate_class, season, time_of_use, avg(price_per_kwh) AS avg_price FROM {s}.tariff_rates GROUP BY rate_class, season, time_of_use"),
        ("v_thermostat_savings", "SELECT mode, count(*) AS readings, avg(energy_saved_kwh) AS avg_saved FROM {s}.smart_thermostats GROUP BY mode"),
        ("v_cert_market", "SELECT cert_type, vintage_year, count(*) AS certs, avg(price) AS avg_price FROM {s}.renewable_certificates GROUP BY cert_type, vintage_year"),
        ("v_transmission_age", "SELECT voltage_kv, status, count(*) AS lines, avg(2025-year_built) AS avg_age FROM {s}.transmission_lines GROUP BY voltage_kv, status"),
        ("v_meter_quality", "SELECT reading_type, quality, count(*) AS readings FROM {s}.meter_readings GROUP BY reading_type, quality"),
        ("v_plant_utilization", "SELECT pp.fuel_type, pp.status, avg(go.capacity_factor) AS avg_cf FROM {s}.power_plants pp JOIN {s}.generation_output go ON pp.plant_id = go.plant_id GROUP BY pp.fuel_type, pp.status"),
        ("v_substation_load", "SELECT station_type, status, count(*) AS stations, avg(capacity_mva) AS avg_capacity FROM {s}.substations GROUP BY station_type, status"),
    ],
    "udfs": [
        ("format_mwh", "mwh DOUBLE", "STRING", "Formats MWh", "CASE WHEN mwh >= 1000 THEN concat(round(mwh/1000,1),' GWh') ELSE concat(round(mwh,1),' MWh') END"),
        ("carbon_intensity", "co2_tons DOUBLE, mwh DOUBLE", "DECIMAL(8,4)", "CO2 intensity kg/MWh", "round(co2_tons*1000/GREATEST(mwh,1),4)"),
        ("rate_class_label", "code STRING", "STRING", "Rate class label", "CASE WHEN code LIKE 'R%' THEN 'Residential' WHEN code LIKE 'C%' THEN 'Commercial' WHEN code LIKE 'I%' THEN 'Industrial' ELSE 'Other' END"),
        ("grid_frequency_status", "freq DOUBLE", "STRING", "Grid frequency status", "CASE WHEN abs(freq-60) < 0.02 THEN 'Normal' WHEN abs(freq-60) < 0.05 THEN 'Alert' ELSE 'Emergency' END"),
        ("solar_degradation", "capacity DECIMAL(8,2), rate DECIMAL(5,4), years INT", "DECIMAL(8,2)", "Degraded solar capacity", "round(capacity * power(1-rate, years), 2)"),
        ("battery_soh", "cycles INT, max_cycles INT", "DECIMAL(5,2)", "Battery state of health", "round((1 - cycles*1.0/GREATEST(max_cycles,1)) * 100, 2)"),
        ("peak_pricing", "hour INT, season STRING", "STRING", "Peak pricing tier", "CASE WHEN season='summer' AND hour BETWEEN 14 AND 19 THEN 'Super Peak' WHEN hour BETWEEN 7 AND 21 THEN 'Peak' ELSE 'Off-Peak' END"),
        ("emission_compliance", "actual DOUBLE, limit_val DOUBLE", "STRING", "Compliance status", "CASE WHEN actual <= limit_val*0.8 THEN 'Well Below' WHEN actual <= limit_val THEN 'Compliant' ELSE 'Exceeds Limit' END"),
        ("ev_charge_cost", "kwh DOUBLE, rate DECIMAL(8,6)", "DECIMAL(8,2)", "EV charging cost", "round(kwh*rate, 2)"),
        ("thermostat_mode_icon", "mode STRING", "STRING", "Thermostat mode", "CASE WHEN mode='heat' THEN '🔥 Heat' WHEN mode='cool' THEN '❄️ Cool' WHEN mode='eco' THEN '🌿 Eco' WHEN mode='auto' THEN '🔄 Auto' ELSE '⏸ Off' END"),
        ("outage_severity", "customers INT, duration INT", "STRING", "Outage severity", "CASE WHEN customers > 10000 OR duration > 240 THEN 'Major' WHEN customers > 1000 OR duration > 60 THEN 'Significant' ELSE 'Minor' END"),
        ("voltage_level", "kv INT", "STRING", "Voltage classification", "CASE WHEN kv >= 345 THEN 'Extra High' WHEN kv >= 115 THEN 'High' WHEN kv >= 35 THEN 'Medium' ELSE 'Low' END"),
        ("fuel_color", "fuel STRING", "STRING", "Fuel type color code", "CASE WHEN fuel IN ('solar','wind','hydro','geothermal') THEN 'Green' WHEN fuel = 'nuclear' THEN 'Blue' WHEN fuel = 'natural_gas' THEN 'Gray' ELSE 'Brown' END"),
        ("capacity_factor_grade", "cf DECIMAL(5,2)", "STRING", "Capacity factor grade", "CASE WHEN cf >= 80 THEN 'A' WHEN cf >= 60 THEN 'B' WHEN cf >= 40 THEN 'C' WHEN cf >= 20 THEN 'D' ELSE 'F' END"),
        ("format_co2", "tons DOUBLE", "STRING", "Format CO2 display", "CASE WHEN tons >= 1000000 THEN concat(round(tons/1000000,1),' MT') WHEN tons >= 1000 THEN concat(round(tons/1000,1),' KT') ELSE concat(round(tons,1),' T') END"),
        ("is_renewable", "fuel STRING", "BOOLEAN", "Check if fuel is renewable", "fuel IN ('solar','wind','hydro','geothermal','biomass')"),
        ("demand_response_savings", "reduction_mw DOUBLE, hours INT, price DECIMAL(8,2)", "DECIMAL(10,2)", "DR event savings", "round(reduction_mw * hours * price, 2)"),
        ("line_loss_estimate", "distance_miles DOUBLE, voltage_kv INT", "DECIMAL(5,2)", "Estimated line loss %", "round(distance_miles * 0.01 * (765.0 / GREATEST(voltage_kv, 1)), 2)"),
        ("meter_read_quality", "is_estimated BOOLEAN, quality STRING", "STRING", "Meter read quality label", "CASE WHEN is_estimated THEN 'Estimated' WHEN quality = 'validated' THEN 'Validated' ELSE 'Actual' END"),
        ("anonymize_meter", "meter_id BIGINT", "STRING", "Anonymized meter ref", "concat('MTR-',substring(sha2(cast(meter_id as STRING),256),1,8))"),
    ],
}

# Education, Real Estate, Logistics, Insurance — following same pattern
# (abbreviated table definitions for conciseness)

INDUSTRIES["education"] = {
    "tables": [
        _quick_table("enrollments", 100_000_000, "enrollment_id BIGINT, student_id BIGINT, course_id BIGINT, semester STRING, grade STRING, credits INT, enrollment_date DATE, status STRING, gpa DECIMAL(3,2), instructor_id BIGINT",
            "id + {offset} AS enrollment_id, floor(rand()*2000000)+1 AS student_id, floor(rand()*50000)+1 AS course_id, element_at(array('Fall 2023','Spring 2024','Summer 2024','Fall 2024','Spring 2025'),cast(floor(rand()*5)+1 as INT)) AS semester, element_at(array('A','A-','B+','B','B-','C+','C','C-','D','F','W','I'),cast(floor(rand()*12)+1 as INT)) AS grade, element_at(array(1,2,3,4),cast(floor(rand()*4)+1 as INT)) AS credits, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS enrollment_date, element_at(array('active','completed','withdrawn','dropped'),cast(floor(rand()*4)+1 as INT)) AS status, round(rand()*3+1,2) AS gpa, floor(rand()*5000)+1 AS instructor_id"),
        _quick_table("learning_events", 50_000_000, "event_id BIGINT, student_id BIGINT, course_id BIGINT, event_type STRING, content_id BIGINT, duration_seconds INT, event_timestamp TIMESTAMP, device STRING, score DECIMAL(5,2), completion_pct DECIMAL(5,2)",
            "id + {offset} AS event_id, floor(rand()*2000000)+1 AS student_id, floor(rand()*50000)+1 AS course_id, element_at(array('video_view','quiz_attempt','assignment_submit','discussion_post','reading','lab_activity','exam','peer_review'),cast(floor(rand()*8)+1 as INT)) AS event_type, floor(rand()*100000)+1 AS content_id, cast(floor(rand()*7200)+30 as INT) AS duration_seconds, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp, element_at(array('laptop','tablet','phone','desktop'),cast(floor(rand()*4)+1 as INT)) AS device, round(rand()*100,2) AS score, round(rand()*100,2) AS completion_pct"),
        _quick_table("assessments", 30_000_000, "assessment_id BIGINT, student_id BIGINT, course_id BIGINT, assessment_type STRING, score DECIMAL(5,2), max_score DECIMAL(5,2), submitted_at TIMESTAMP, graded_at TIMESTAMP, grader_id BIGINT, feedback STRING",
            "id + {offset} AS assessment_id, floor(rand()*2000000)+1 AS student_id, floor(rand()*50000)+1 AS course_id, element_at(array('midterm','final','quiz','homework','project','lab','presentation','paper'),cast(floor(rand()*8)+1 as INT)) AS assessment_type, round(rand()*100,2) AS score, 100.0 AS max_score, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS submitted_at, dateadd(SECOND, cast(floor(rand()*1825*86400)+86400 as INT), '2020-01-01') AS graded_at, floor(rand()*5000)+1 AS grader_id, element_at(array('Good work','Needs improvement','Excellent','See comments','Incomplete'),cast(floor(rand()*5)+1 as INT)) AS feedback"),
        _quick_table("students", 1_000_000, "student_id BIGINT, first_name STRING, last_name STRING, email STRING, date_of_birth DATE, major STRING, enrollment_year INT, status STRING, gpa DECIMAL(3,2), financial_aid BOOLEAN",
            "id + {offset} AS student_id, element_at(array('Emma','Liam','Sophia','Noah','Olivia','James','Ava','William','Isabella','Mason'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, concat('student',id + {offset},'@university.edu') AS email, date_add('1998-01-01',cast(floor(rand()*2920) as INT)) AS date_of_birth, element_at(array('Computer Science','Business','Engineering','Biology','Psychology','English','Mathematics','Chemistry','History','Art'),cast(floor(rand()*10)+1 as INT)) AS major, cast(floor(rand()*5)+2020 as INT) AS enrollment_year, element_at(array('active','graduated','on_leave','suspended','transferred'),cast(floor(rand()*5)+1 as INT)) AS status, round(rand()*3+1,2) AS gpa, rand() > 0.4 AS financial_aid"),
        _quick_table("courses", 1_000_000, "course_id BIGINT, code STRING, title STRING, department STRING, credits INT, level STRING, capacity INT, instructor_id BIGINT, is_online BOOLEAN, prerequisite_id BIGINT",
            "id + {offset} AS course_id, concat(element_at(array('CS','BUS','ENG','BIO','PSY','MTH','CHM','HIS','ART','PHY'),cast(floor(rand()*10)+1 as INT)),lpad(cast(floor(rand()*499)+101 as STRING),3,'0')) AS code, concat(element_at(array('Introduction to','Advanced','Principles of','Foundations of','Topics in'),cast(floor(rand()*5)+1 as INT)),' ',element_at(array('Algorithms','Marketing','Thermodynamics','Genetics','Cognition','Statistics','Reactions','Civilizations','Design','Mechanics'),cast(floor(rand()*10)+1 as INT))) AS title, element_at(array('Computer Science','Business','Engineering','Biology','Psychology','Mathematics','Chemistry','History','Art','Physics'),cast(floor(rand()*10)+1 as INT)) AS department, element_at(array(1,2,3,4),cast(floor(rand()*4)+1 as INT)) AS credits, element_at(array('100-level','200-level','300-level','400-level','graduate'),cast(floor(rand()*5)+1 as INT)) AS level, cast(floor(rand()*200)+20 as INT) AS capacity, floor(rand()*5000)+1 AS instructor_id, rand() > 0.7 AS is_online, CASE WHEN rand() > 0.5 THEN floor(rand()*50000)+1 ELSE NULL END AS prerequisite_id"),
        _quick_table("instructors", 500_000, "instructor_id BIGINT, first_name STRING, last_name STRING, email STRING, department STRING, title STRING, tenure BOOLEAN, hire_date DATE, research_area STRING, rating DECIMAL(3,1)",
            "id + {offset} AS instructor_id, element_at(array('Sarah','David','Emily','James','Anna','Robert','Maria','William','Lisa','Thomas'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Chen','Patel','Kim','Singh','Lee','Wang','Kumar','Shah','Ali','Gupta'),cast(floor(rand()*10)+1 as INT)) AS last_name, concat('prof',id + {offset},'@university.edu') AS email, element_at(array('Computer Science','Business','Engineering','Biology','Psychology'),cast(floor(rand()*5)+1 as INT)) AS department, element_at(array('Professor','Associate Professor','Assistant Professor','Lecturer','Adjunct'),cast(floor(rand()*5)+1 as INT)) AS title, rand() > 0.6 AS tenure, date_add('2000-01-01',cast(floor(rand()*9125) as INT)) AS hire_date, element_at(array('AI/ML','Data Science','Marketing','Quantum','Neuroscience','Topology','Organic Chemistry','Medieval','Digital Art','Relativity'),cast(floor(rand()*10)+1 as INT)) AS research_area, round(rand()*3+2,1) AS rating"),
        _quick_table("financial_aid", 500_000, "aid_id BIGINT, student_id BIGINT, aid_type STRING, amount DECIMAL(10,2), academic_year STRING, status STRING, disbursement_date DATE, gpa_requirement DECIMAL(3,2)",
            "id + {offset} AS aid_id, floor(rand()*2000000)+1 AS student_id, element_at(array('scholarship','grant','loan','work_study','fellowship','tuition_waiver'),cast(floor(rand()*6)+1 as INT)) AS aid_type, round(rand()*30000+500,2) AS amount, concat('20',cast(floor(rand()*5)+20 as STRING),'-',cast(floor(rand()*5)+21 as STRING)) AS academic_year, element_at(array('approved','pending','disbursed','cancelled','returned'),cast(floor(rand()*5)+1 as INT)) AS status, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS disbursement_date, round(rand()*2+2,2) AS gpa_requirement"),
        _quick_table("facilities_edu", 300_000, "facility_id BIGINT, name STRING, building STRING, facility_type STRING, capacity INT, has_av BOOLEAN, is_accessible BOOLEAN, floor_num INT",
            "id + {offset} AS facility_id, concat(element_at(array('Room','Lab','Hall','Studio','Theater','Arena','Center'),cast(floor(rand()*7)+1 as INT)),' ',lpad(cast(floor(rand()*999)+100 as STRING),3,'0')) AS name, element_at(array('Science Hall','Engineering Bldg','Business Center','Arts Building','Library','Student Union','Gymnasium','Medical Center'),cast(floor(rand()*8)+1 as INT)) AS building, element_at(array('classroom','laboratory','lecture_hall','seminar','computer_lab','studio','auditorium'),cast(floor(rand()*7)+1 as INT)) AS facility_type, cast(floor(rand()*500)+10 as INT) AS capacity, rand() > 0.3 AS has_av, rand() > 0.2 AS is_accessible, cast(floor(rand()*5)+1 as INT) AS floor_num"),
        _quick_table("research_grants", 200_000, "grant_id BIGINT, pi_id BIGINT, title STRING, funding_agency STRING, amount DECIMAL(12,2), start_date DATE, end_date DATE, status STRING, department STRING",
            "id + {offset} AS grant_id, floor(rand()*5000)+1 AS pi_id, concat('Research on ',element_at(array('AI','Climate','Genomics','Quantum','Neural','Materials','Energy','Disease','Space','Ocean'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('Systems','Models','Processes','Dynamics','Networks'),cast(floor(rand()*5)+1 as INT))) AS title, element_at(array('NSF','NIH','DOE','DARPA','NASA','DOD','Private Foundation','Industry'),cast(floor(rand()*8)+1 as INT)) AS funding_agency, round(rand()*2000000+10000,2) AS amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS start_date, date_add('2022-01-01',cast(floor(rand()*1825) as INT)) AS end_date, element_at(array('active','completed','pending','expired'),cast(floor(rand()*4)+1 as INT)) AS status, element_at(array('Computer Science','Biology','Engineering','Physics','Chemistry'),cast(floor(rand()*5)+1 as INT)) AS department"),
        _quick_table("library_checkouts", 200_000, "checkout_id BIGINT, student_id BIGINT, item_id BIGINT, item_type STRING, checkout_date DATE, due_date DATE, return_date DATE, renewals INT, status STRING",
            "id + {offset} AS checkout_id, floor(rand()*2000000)+1 AS student_id, floor(rand()*500000)+1 AS item_id, element_at(array('book','journal','media','equipment','reserve'),cast(floor(rand()*5)+1 as INT)) AS item_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS checkout_date, date_add('2020-01-01',cast(floor(rand()*1825)+14 as INT)) AS due_date, date_add('2020-01-01',cast(floor(rand()*1825)+10 as INT)) AS return_date, cast(floor(rand()*3) as INT) AS renewals, element_at(array('returned','checked_out','overdue','lost'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("alumni", 200_000, "alumni_id BIGINT, student_id BIGINT, graduation_year INT, degree STRING, major STRING, employer STRING, job_title STRING, salary_range STRING, donated BOOLEAN, donation_total DECIMAL(10,2)",
            "id + {offset} AS alumni_id, floor(rand()*2000000)+1 AS student_id, cast(floor(rand()*30)+1995 as INT) AS graduation_year, element_at(array('BS','BA','MS','MA','PhD','MBA','MD','JD'),cast(floor(rand()*8)+1 as INT)) AS degree, element_at(array('Computer Science','Business','Engineering','Biology','Psychology'),cast(floor(rand()*5)+1 as INT)) AS major, element_at(array('Google','Amazon','Microsoft','Meta','Apple','JPMorgan','McKinsey','Deloitte','Hospital','University'),cast(floor(rand()*10)+1 as INT)) AS employer, element_at(array('Software Engineer','Analyst','Manager','Director','VP','Researcher','Consultant','Doctor','Professor'),cast(floor(rand()*9)+1 as INT)) AS job_title, element_at(array('0-50k','50-100k','100-150k','150-200k','200k+'),cast(floor(rand()*5)+1 as INT)) AS salary_range, rand() > 0.6 AS donated, round(rand()*50000,2) AS donation_total"),
        _quick_table("campus_events", 100_000, "event_id BIGINT, title STRING, event_type STRING, organizer STRING, venue_id BIGINT, event_date DATE, attendees INT, budget DECIMAL(8,2), status STRING",
            "id + {offset} AS event_id, concat(element_at(array('Annual','Spring','Fall','Winter','Summer'),cast(floor(rand()*5)+1 as INT)),' ',element_at(array('Conference','Seminar','Workshop','Festival','Career Fair','Lecture','Hackathon','Symposium'),cast(floor(rand()*8)+1 as INT))) AS title, element_at(array('academic','social','athletic','career','cultural','research'),cast(floor(rand()*6)+1 as INT)) AS event_type, concat('Dept of ',element_at(array('CS','Business','Engineering','Student Affairs','Athletics'),cast(floor(rand()*5)+1 as INT))) AS organizer, floor(rand()*300)+1 AS venue_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS event_date, cast(floor(rand()*5000)+10 as INT) AS attendees, round(rand()*50000+100,2) AS budget, element_at(array('scheduled','completed','cancelled','in_progress'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("dining_transactions", 300_000, "txn_id BIGINT, student_id BIGINT, location STRING, amount DECIMAL(6,2), txn_date DATE, meal_type STRING, payment_type STRING",
            "id + {offset} AS txn_id, floor(rand()*2000000)+1 AS student_id, element_at(array('Main Dining Hall','Cafe','Food Court','Snack Bar','Coffee Shop'),cast(floor(rand()*5)+1 as INT)) AS location, round(rand()*20+2,2) AS amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS txn_date, element_at(array('breakfast','lunch','dinner','snack'),cast(floor(rand()*4)+1 as INT)) AS meal_type, element_at(array('meal_plan','flex_dollars','cash','credit_card'),cast(floor(rand()*4)+1 as INT)) AS payment_type"),
        _quick_table("housing", 200_000, "assignment_id BIGINT, student_id BIGINT, building STRING, room_number STRING, room_type STRING, semester STRING, monthly_rate DECIMAL(8,2), status STRING",
            "id + {offset} AS assignment_id, floor(rand()*2000000)+1 AS student_id, element_at(array('Oak Hall','Maple Dorm','Pine Residence','Cedar Towers','Elm Suites'),cast(floor(rand()*5)+1 as INT)) AS building, concat(cast(floor(rand()*4)+1 as STRING),lpad(cast(floor(rand()*50)+1 as STRING),2,'0')) AS room_number, element_at(array('single','double','triple','suite','apartment'),cast(floor(rand()*5)+1 as INT)) AS room_type, element_at(array('Fall 2023','Spring 2024','Summer 2024','Fall 2024'),cast(floor(rand()*4)+1 as INT)) AS semester, round(rand()*1500+500,2) AS monthly_rate, element_at(array('assigned','vacant','maintenance','reserved'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("parking_permits", 100_000, "permit_id BIGINT, holder_id BIGINT, holder_type STRING, lot STRING, vehicle_plate STRING, start_date DATE, end_date DATE, cost DECIMAL(6,2), status STRING",
            "id + {offset} AS permit_id, floor(rand()*500000)+1 AS holder_id, element_at(array('student','faculty','staff','visitor'),cast(floor(rand()*4)+1 as INT)) AS holder_type, element_at(array('Lot A','Lot B','Lot C','Garage 1','Garage 2'),cast(floor(rand()*5)+1 as INT)) AS lot, upper(substring(sha2(cast(id as STRING),256),1,7)) AS vehicle_plate, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS start_date, date_add('2024-06-01',cast(floor(rand()*365) as INT)) AS end_date, round(rand()*500+50,2) AS cost, element_at(array('active','expired','revoked'),cast(floor(rand()*3)+1 as INT)) AS status"),
        _quick_table("athletics", 100_000, "event_id BIGINT, sport STRING, event_date DATE, opponent STRING, home_score INT, away_score INT, attendance INT, venue STRING, season STRING, result STRING",
            "id + {offset} AS event_id, element_at(array('Football','Basketball','Baseball','Soccer','Tennis','Swimming','Track','Volleyball','Lacrosse','Hockey'),cast(floor(rand()*10)+1 as INT)) AS sport, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS event_date, concat('University of ',element_at(array('State','Tech','Valley','Coast','Mountain','Lake','Plains','Central','Northern','Southern'),cast(floor(rand()*10)+1 as INT))) AS opponent, cast(floor(rand()*100) as INT) AS home_score, cast(floor(rand()*100) as INT) AS away_score, cast(floor(rand()*80000)+100 as INT) AS attendance, element_at(array('Stadium','Arena','Field','Court','Pool','Track','Gymnasium'),cast(floor(rand()*7)+1 as INT)) AS venue, concat(cast(floor(rand()*5)+2020 as STRING),'-',cast(floor(rand()*5)+2021 as STRING)) AS season, element_at(array('win','loss','tie','cancelled'),cast(floor(rand()*4)+1 as INT)) AS result"),
        _quick_table("it_helpdesk", 100_000, "ticket_id BIGINT, requester_id BIGINT, requester_type STRING, category STRING, priority STRING, created_date DATE, resolved_date DATE, status STRING, satisfaction INT",
            "id + {offset} AS ticket_id, floor(rand()*500000)+1 AS requester_id, element_at(array('student','faculty','staff'),cast(floor(rand()*3)+1 as INT)) AS requester_type, element_at(array('network','email','software','hardware','account','lms','printing','security'),cast(floor(rand()*8)+1 as INT)) AS category, element_at(array('critical','high','medium','low'),cast(floor(rand()*4)+1 as INT)) AS priority, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS created_date, date_add('2020-01-01',cast(floor(rand()*1825)+2 as INT)) AS resolved_date, element_at(array('open','in_progress','resolved','closed'),cast(floor(rand()*4)+1 as INT)) AS status, cast(floor(rand()*5)+1 as INT) AS satisfaction"),
        _quick_table("departments", 100_000, "dept_id BIGINT, name STRING, college STRING, head_id BIGINT, budget DECIMAL(12,2), faculty_count INT, student_count INT, established_year INT",
            "id + {offset} AS dept_id, element_at(array('Computer Science','Mathematics','Physics','Chemistry','Biology','English','History','Psychology','Business','Engineering','Art','Music','Philosophy','Economics','Sociology','Political Science','Nursing','Education','Law','Medicine'),cast(floor(rand()*20)+1 as INT)) AS name, element_at(array('Arts & Sciences','Engineering','Business','Health Sciences','Education','Law','Fine Arts'),cast(floor(rand()*7)+1 as INT)) AS college, floor(rand()*5000)+1 AS head_id, round(rand()*10000000+100000,2) AS budget, cast(floor(rand()*100)+5 as INT) AS faculty_count, cast(floor(rand()*2000)+50 as INT) AS student_count, cast(floor(rand()*100)+1900 as INT) AS established_year"),
    ],
    "views": [
        ("v_student_gpa", "SELECT student_id, avg(gpa) AS avg_gpa, count(*) AS courses FROM {s}.enrollments WHERE status = 'completed' GROUP BY student_id"),
        ("v_course_enrollment", "SELECT c.code, c.title, c.department, count(e.enrollment_id) AS enrolled, c.capacity FROM {s}.courses c LEFT JOIN {s}.enrollments e ON c.course_id = e.course_id GROUP BY c.code, c.title, c.department, c.capacity"),
        ("v_department_summary", "SELECT department, count(DISTINCT student_id) AS students, count(DISTINCT course_id) AS courses, avg(gpa) AS avg_gpa FROM {s}.enrollments e JOIN {s}.courses c ON e.course_id = c.course_id GROUP BY department"),
        ("v_instructor_ratings", "SELECT i.first_name, i.last_name, i.department, i.rating, count(DISTINCT e.student_id) AS students_taught FROM {s}.instructors i LEFT JOIN {s}.courses c ON i.instructor_id = c.instructor_id LEFT JOIN {s}.enrollments e ON c.course_id = e.course_id GROUP BY i.first_name, i.last_name, i.department, i.rating"),
        ("v_financial_aid_summary", "SELECT aid_type, academic_year, count(*) AS awards, sum(amount) AS total_awarded FROM {s}.financial_aid GROUP BY aid_type, academic_year"),
        ("v_research_funding", "SELECT department, funding_agency, count(*) AS grants, sum(amount) AS total_funding FROM {s}.research_grants WHERE status = 'active' GROUP BY department, funding_agency"),
        ("v_retention_rate", "SELECT enrollment_year, status, count(*) AS students FROM {s}.students GROUP BY enrollment_year, status"),
        ("v_learning_engagement", "SELECT event_type, device, count(*) AS events, avg(duration_seconds) AS avg_duration FROM {s}.learning_events GROUP BY event_type, device"),
        ("v_assessment_results", "SELECT assessment_type, avg(score) AS avg_score, avg(score/max_score*100) AS avg_pct FROM {s}.assessments GROUP BY assessment_type"),
        ("v_alumni_outcomes", "SELECT major, degree, avg(CASE salary_range WHEN '200k+' THEN 225000 WHEN '150-200k' THEN 175000 WHEN '100-150k' THEN 125000 WHEN '50-100k' THEN 75000 ELSE 25000 END) AS est_avg_salary FROM {s}.alumni GROUP BY major, degree"),
        ("v_library_usage", "SELECT item_type, status, count(*) AS checkouts FROM {s}.library_checkouts GROUP BY item_type, status"),
        ("v_housing_occupancy", "SELECT building, room_type, count(*) AS rooms, sum(CASE WHEN status='assigned' THEN 1 ELSE 0 END) AS occupied FROM {s}.housing GROUP BY building, room_type"),
        ("v_dining_revenue", "SELECT location, meal_type, count(*) AS transactions, sum(amount) AS revenue FROM {s}.dining_transactions GROUP BY location, meal_type"),
        ("v_athletic_record", "SELECT sport, result, count(*) AS games FROM {s}.athletics GROUP BY sport, result"),
        ("v_it_support_metrics", "SELECT category, priority, avg(datediff(resolved_date,created_date)) AS avg_resolution_days, avg(satisfaction) AS avg_satisfaction FROM {s}.it_helpdesk WHERE status IN ('resolved','closed') GROUP BY category, priority"),
        ("v_campus_events_calendar", "SELECT event_type, date_trunc('month',event_date) AS month, count(*) AS events, sum(attendees) AS total_attendees FROM {s}.campus_events GROUP BY event_type, date_trunc('month',event_date)"),
        ("v_parking_utilization", "SELECT lot, holder_type, count(*) AS permits FROM {s}.parking_permits WHERE status='active' GROUP BY lot, holder_type"),
        ("v_major_popularity", "SELECT major, count(*) AS students, avg(gpa) AS avg_gpa FROM {s}.students WHERE status='active' GROUP BY major"),
        ("v_grant_by_agency", "SELECT funding_agency, count(*) AS grants, sum(amount) AS total, avg(amount) AS avg_grant FROM {s}.research_grants GROUP BY funding_agency"),
        ("v_facility_usage", "SELECT facility_type, count(*) AS rooms, avg(capacity) AS avg_capacity FROM {s}.facilities_edu GROUP BY facility_type"),
    ],
    "udfs": [
        ("gpa_to_letter", "gpa DECIMAL(3,2)", "STRING", "Converts GPA to letter grade", "CASE WHEN gpa >= 3.7 THEN 'A' WHEN gpa >= 3.3 THEN 'A-' WHEN gpa >= 3.0 THEN 'B+' WHEN gpa >= 2.7 THEN 'B' WHEN gpa >= 2.3 THEN 'B-' WHEN gpa >= 2.0 THEN 'C+' WHEN gpa >= 1.7 THEN 'C' WHEN gpa >= 1.0 THEN 'D' ELSE 'F' END"),
        ("academic_standing", "gpa DECIMAL(3,2)", "STRING", "Academic standing", "CASE WHEN gpa >= 3.5 THEN 'Deans List' WHEN gpa >= 2.0 THEN 'Good Standing' WHEN gpa >= 1.5 THEN 'Probation' ELSE 'Suspension Risk' END"),
        ("credit_hours_status", "credits INT, year INT", "STRING", "Credit progress", "CASE WHEN credits >= 120 THEN 'Senior' WHEN credits >= 90 THEN 'Junior' WHEN credits >= 60 THEN 'Sophomore' ELSE 'Freshman' END"),
        ("mask_student_email", "email STRING", "STRING", "Masks student email", "concat(left(email,3),'***@university.edu')"),
        ("semester_label", "sem STRING", "STRING", "Formatted semester", "sem"),
        ("is_stem", "major STRING", "BOOLEAN", "STEM major check", "major IN ('Computer Science','Engineering','Mathematics','Physics','Chemistry','Biology')"),
        ("aid_per_semester", "amount DECIMAL(10,2), semesters INT", "DECIMAL(10,2)", "Aid per semester", "round(amount / GREATEST(semesters, 1), 2)"),
        ("course_level", "code STRING", "STRING", "Course level from code", "CASE WHEN cast(right(code,3) as INT) >= 500 THEN 'Graduate' WHEN cast(right(code,3) as INT) >= 400 THEN 'Senior' WHEN cast(right(code,3) as INT) >= 300 THEN 'Junior' WHEN cast(right(code,3) as INT) >= 200 THEN 'Sophomore' ELSE 'Freshman' END"),
        ("graduation_eligible", "credits INT, gpa DECIMAL(3,2)", "BOOLEAN", "Graduation eligibility", "credits >= 120 AND gpa >= 2.0"),
        ("format_grade", "score DECIMAL(5,2), max_score DECIMAL(5,2)", "STRING", "Format as percentage", "concat(cast(round(score/max_score*100,1) as STRING),'%')"),
        ("research_status", "start_date DATE, end_date DATE", "STRING", "Grant status", "CASE WHEN current_date() < start_date THEN 'Not Started' WHEN current_date() > end_date THEN 'Completed' ELSE 'Active' END"),
        ("housing_cost_semester", "monthly DECIMAL(8,2)", "DECIMAL(8,2)", "Semester housing cost", "round(monthly * 4.5, 2)"),
        ("meal_plan_remaining", "total DECIMAL(8,2), spent DECIMAL(8,2)", "STRING", "Meal plan balance", "concat('$',format_number(total-spent,2),' remaining')"),
        ("anonymize_student", "first_name STRING, student_id BIGINT", "STRING", "Anonymized student ref", "concat('STU-',substring(sha2(concat(first_name,cast(student_id as STRING)),256),1,8))"),
        ("instructor_title_rank", "title STRING", "INT", "Rank by title", "CASE WHEN title = 'Professor' THEN 5 WHEN title = 'Associate Professor' THEN 4 WHEN title = 'Assistant Professor' THEN 3 WHEN title = 'Lecturer' THEN 2 ELSE 1 END"),
        ("is_overdue", "due_date DATE, return_date DATE", "BOOLEAN", "Check if library item overdue", "return_date IS NULL OR return_date > due_date"),
        ("event_size", "attendees INT", "STRING", "Event size category", "CASE WHEN attendees > 5000 THEN 'Large' WHEN attendees > 500 THEN 'Medium' WHEN attendees > 50 THEN 'Small' ELSE 'Micro' END"),
        ("ticket_sla", "priority STRING, days INT", "BOOLEAN", "SLA met check", "days <= CASE WHEN priority = 'critical' THEN 1 WHEN priority = 'high' THEN 3 WHEN priority = 'medium' THEN 5 ELSE 10 END"),
        ("donation_tier", "total DECIMAL(10,2)", "STRING", "Donor tier", "CASE WHEN total >= 10000 THEN 'Platinum' WHEN total >= 5000 THEN 'Gold' WHEN total >= 1000 THEN 'Silver' ELSE 'Bronze' END"),
        ("class_year", "enrollment_year INT", "STRING", "Expected graduation", "concat('Class of ',enrollment_year + 4)"),
    ],
}

# Remaining 3 industries use similar compact patterns
INDUSTRIES["real_estate"] = {
    "tables": [
        _quick_table("listings", 100_000_000, "listing_id BIGINT, property_id BIGINT, list_price DECIMAL(12,2), list_date DATE, status STRING, agent_id BIGINT, property_type STRING, bedrooms INT, bathrooms DECIMAL(3,1), sqft INT",
            "id + {offset} AS listing_id, floor(rand()*5000000)+1 AS property_id, round(rand()*2000000+50000,2) AS list_price, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS list_date, element_at(array('active','pending','sold','expired','withdrawn'),cast(floor(rand()*5)+1 as INT)) AS status, floor(rand()*100000)+1 AS agent_id, element_at(array('single_family','condo','townhouse','multi_family','land','commercial'),cast(floor(rand()*6)+1 as INT)) AS property_type, cast(floor(rand()*6)+1 as INT) AS bedrooms, round(floor(rand()*6)+1,1) AS bathrooms, cast(floor(rand()*5000)+500 as INT) AS sqft"),
        _quick_table("transactions_re", 50_000_000, "txn_id BIGINT, listing_id BIGINT, buyer_id BIGINT, seller_id BIGINT, sale_price DECIMAL(12,2), closing_date DATE, mortgage_amount DECIMAL(12,2), down_payment_pct DECIMAL(5,2), agent_commission DECIMAL(10,2), days_on_market INT",
            "id + {offset} AS txn_id, floor(rand()*100000000)+1 AS listing_id, floor(rand()*2000000)+1 AS buyer_id, floor(rand()*2000000)+1 AS seller_id, round(rand()*2000000+50000,2) AS sale_price, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS closing_date, round(rand()*1500000+40000,2) AS mortgage_amount, round(rand()*30+3,2) AS down_payment_pct, round(rand()*60000+2000,2) AS agent_commission, cast(floor(rand()*365)+1 as INT) AS days_on_market"),
        _quick_table("property_views", 30_000_000, "view_id BIGINT, listing_id BIGINT, viewer_id BIGINT, view_type STRING, view_date DATE, duration_minutes INT, source STRING, saved BOOLEAN, inquired BOOLEAN",
            "id + {offset} AS view_id, floor(rand()*100000000)+1 AS listing_id, floor(rand()*5000000)+1 AS viewer_id, element_at(array('online','in_person','virtual_tour','open_house'),cast(floor(rand()*4)+1 as INT)) AS view_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS view_date, cast(floor(rand()*60)+1 as INT) AS duration_minutes, element_at(array('zillow','realtor','redfin','mls','agent','social'),cast(floor(rand()*6)+1 as INT)) AS source, rand() > 0.7 AS saved, rand() > 0.8 AS inquired"),
        _quick_table("properties", 1_000_000, "property_id BIGINT, address STRING, city STRING, state STRING, zip STRING, property_type STRING, year_built INT, lot_size_sqft INT, assessed_value DECIMAL(12,2), last_sale_date DATE",
            "id + {offset} AS property_id, concat(cast(floor(rand()*9999)+1 as STRING),' ',element_at(array('Oak','Maple','Cedar','Pine','Elm','Main','Park','Lake','Hill','Valley'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('St','Ave','Blvd','Dr','Ln','Way','Ct','Rd'),cast(floor(rand()*8)+1 as INT))) AS address, element_at(array('Austin','Denver','Nashville','Raleigh','Portland','Charlotte','Tampa','Phoenix','Dallas','Atlanta'),cast(floor(rand()*10)+1 as INT)) AS city, element_at(array('TX','CO','TN','NC','OR','NC','FL','AZ','TX','GA'),cast(floor(rand()*10)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip, element_at(array('single_family','condo','townhouse','multi_family','land','commercial'),cast(floor(rand()*6)+1 as INT)) AS property_type, cast(floor(rand()*70)+1955 as INT) AS year_built, cast(floor(rand()*50000)+1000 as INT) AS lot_size_sqft, round(rand()*2000000+30000,2) AS assessed_value, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS last_sale_date"),
        _quick_table("agents", 1_000_000, "agent_id BIGINT, first_name STRING, last_name STRING, brokerage STRING, license_number STRING, email STRING, phone STRING, specialization STRING, years_experience INT, listings_active INT",
            "id + {offset} AS agent_id, element_at(array('Sarah','David','Emily','James','Anna','Robert','Maria','William','Lisa','Thomas'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, element_at(array('Keller Williams','RE/MAX','Coldwell Banker','Century 21','Compass','eXp Realty','Sothebys','Berkshire Hathaway'),cast(floor(rand()*8)+1 as INT)) AS brokerage, concat('LIC-',lpad(cast(floor(rand()*999999) as STRING),6,'0')) AS license_number, concat('agent',id + {offset},'@realty.com') AS email, concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone, element_at(array('residential','luxury','commercial','investment','first_time_buyer'),cast(floor(rand()*5)+1 as INT)) AS specialization, cast(floor(rand()*30)+1 as INT) AS years_experience, cast(floor(rand()*50) as INT) AS listings_active"),
        _quick_table("mortgages", 1_000_000, "mortgage_id BIGINT, property_id BIGINT, borrower_id BIGINT, lender STRING, loan_amount DECIMAL(12,2), interest_rate DECIMAL(5,4), term_years INT, loan_type STRING, origination_date DATE, status STRING",
            "id + {offset} AS mortgage_id, floor(rand()*5000000)+1 AS property_id, floor(rand()*2000000)+1 AS borrower_id, element_at(array('Wells Fargo','Chase','BofA','Quicken','US Bank','PennyMac','loanDepot','Guaranteed Rate'),cast(floor(rand()*8)+1 as INT)) AS lender, round(rand()*1500000+50000,2) AS loan_amount, round(rand()*0.05+0.03,4) AS interest_rate, element_at(array(15,20,30),cast(floor(rand()*3)+1 as INT)) AS term_years, element_at(array('conventional','FHA','VA','USDA','jumbo'),cast(floor(rand()*5)+1 as INT)) AS loan_type, date_add('2015-01-01',cast(floor(rand()*3650) as INT)) AS origination_date, element_at(array('active','paid_off','default','refinanced'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("appraisals", 500_000, "appraisal_id BIGINT, property_id BIGINT, appraised_value DECIMAL(12,2), appraisal_date DATE, appraiser_id BIGINT, purpose STRING, condition STRING, market_trend STRING",
            "id + {offset} AS appraisal_id, floor(rand()*5000000)+1 AS property_id, round(rand()*2000000+30000,2) AS appraised_value, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS appraisal_date, floor(rand()*10000)+1 AS appraiser_id, element_at(array('purchase','refinance','heloc','estate','tax_appeal'),cast(floor(rand()*5)+1 as INT)) AS purpose, element_at(array('excellent','good','average','fair','poor'),cast(floor(rand()*5)+1 as INT)) AS condition, element_at(array('increasing','stable','declining'),cast(floor(rand()*3)+1 as INT)) AS market_trend"),
        _quick_table("inspections", 500_000, "inspection_id BIGINT, property_id BIGINT, inspection_date DATE, inspector_id BIGINT, inspection_type STRING, overall_rating STRING, issues_found INT, estimated_repair_cost DECIMAL(10,2), report_url STRING",
            "id + {offset} AS inspection_id, floor(rand()*5000000)+1 AS property_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS inspection_date, floor(rand()*5000)+1 AS inspector_id, element_at(array('general','termite','radon','mold','roof','electrical','plumbing','foundation'),cast(floor(rand()*8)+1 as INT)) AS inspection_type, element_at(array('excellent','good','fair','poor','critical'),cast(floor(rand()*5)+1 as INT)) AS overall_rating, cast(floor(rand()*20) as INT) AS issues_found, round(rand()*50000,2) AS estimated_repair_cost, concat('https://inspections.example.com/report/',id + {offset}) AS report_url"),
        _quick_table("rental_payments", 500_000, "payment_id BIGINT, lease_id BIGINT, tenant_id BIGINT, amount DECIMAL(8,2), payment_date DATE, due_date DATE, payment_method STRING, status STRING, late_fee DECIMAL(6,2)",
            "id + {offset} AS payment_id, floor(rand()*200000)+1 AS lease_id, floor(rand()*500000)+1 AS tenant_id, round(rand()*3000+500,2) AS amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS payment_date, date_add('2020-01-01',cast(floor(rand()*60)*30 as INT)) AS due_date, element_at(array('check','ach','credit_card','cash','money_order'),cast(floor(rand()*5)+1 as INT)) AS payment_method, element_at(array('paid','pending','late','partial','bounced'),cast(floor(rand()*5)+1 as INT)) AS status, CASE WHEN rand() > 0.8 THEN round(rand()*100+25,2) ELSE 0 END AS late_fee"),
        _quick_table("neighborhoods", 500_000, "neighborhood_id BIGINT, name STRING, city STRING, state STRING, zip STRING, median_price DECIMAL(12,2), avg_sqft_price DECIMAL(8,2), school_rating INT, crime_index DECIMAL(5,2), walkability_score INT",
            "id + {offset} AS neighborhood_id, concat(element_at(array('Downtown','Midtown','Uptown','Westside','Eastside','Historic','Lakeside','Riverside','Garden','Heights'),cast(floor(rand()*10)+1 as INT)),' ',element_at(array('District','Quarter','Village','Park','Commons','Green','Square'),cast(floor(rand()*7)+1 as INT))) AS name, element_at(array('Austin','Denver','Nashville','Raleigh','Portland'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('TX','CO','TN','NC','OR'),cast(floor(rand()*5)+1 as INT)) AS state, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip, round(rand()*1000000+100000,2) AS median_price, round(rand()*500+50,2) AS avg_sqft_price, cast(floor(rand()*10)+1 as INT) AS school_rating, round(rand()*100,2) AS crime_index, cast(floor(rand()*100)+1 as INT) AS walkability_score"),
        _quick_table("open_houses", 300_000, "oh_id BIGINT, listing_id BIGINT, agent_id BIGINT, oh_date DATE, start_time STRING, end_time STRING, attendees INT, offers_received INT",
            "id + {offset} AS oh_id, floor(rand()*100000000)+1 AS listing_id, floor(rand()*100000)+1 AS agent_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS oh_date, element_at(array('10:00','11:00','12:00','13:00','14:00'),cast(floor(rand()*5)+1 as INT)) AS start_time, element_at(array('12:00','13:00','14:00','15:00','16:00'),cast(floor(rand()*5)+1 as INT)) AS end_time, cast(floor(rand()*50)+1 as INT) AS attendees, cast(floor(rand()*5) as INT) AS offers_received"),
        _quick_table("market_trends", 200_000, "trend_id BIGINT, zip_code STRING, month DATE, median_price DECIMAL(12,2), avg_days_on_market INT, inventory INT, new_listings INT, closed_sales INT, price_per_sqft DECIMAL(8,2)",
            "id + {offset} AS trend_id, lpad(cast(floor(rand()*99999) as STRING),5,'0') AS zip_code, date_add('2020-01-01',cast(floor(rand()*60)*30 as INT)) AS month, round(rand()*1000000+100000,2) AS median_price, cast(floor(rand()*120)+5 as INT) AS avg_days_on_market, cast(floor(rand()*500)+10 as INT) AS inventory, cast(floor(rand()*200)+5 as INT) AS new_listings, cast(floor(rand()*150)+3 as INT) AS closed_sales, round(rand()*500+50,2) AS price_per_sqft"),
        _quick_table("hoa_dues", 200_000, "hoa_id BIGINT, property_id BIGINT, community STRING, monthly_dues DECIMAL(8,2), special_assessment DECIMAL(8,2), reserve_fund DECIMAL(12,2), paid_through DATE, status STRING",
            "id + {offset} AS hoa_id, floor(rand()*5000000)+1 AS property_id, concat(element_at(array('Sunset','Lakewood','Oak Park','Riverside','Heritage','Vista','Summit','Valley'),cast(floor(rand()*8)+1 as INT)),' HOA') AS community, round(rand()*500+50,2) AS monthly_dues, round(rand()*2000,2) AS special_assessment, round(rand()*500000+10000,2) AS reserve_fund, date_add('2024-01-01',cast(floor(rand()*365) as INT)) AS paid_through, element_at(array('current','delinquent','paid_off'),cast(floor(rand()*3)+1 as INT)) AS status"),
        _quick_table("property_taxes", 200_000, "tax_id BIGINT, property_id BIGINT, tax_year INT, assessed_value DECIMAL(12,2), tax_rate DECIMAL(6,4), tax_amount DECIMAL(10,2), paid_date DATE, status STRING",
            "id + {offset} AS tax_id, floor(rand()*5000000)+1 AS property_id, cast(floor(rand()*5)+2020 as INT) AS tax_year, round(rand()*2000000+30000,2) AS assessed_value, round(rand()*0.03+0.005,4) AS tax_rate, round(rand()*30000+500,2) AS tax_amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS paid_date, element_at(array('paid','due','delinquent','appealed'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("construction_permits", 100_000, "permit_id BIGINT, property_id BIGINT, permit_type STRING, description STRING, estimated_cost DECIMAL(12,2), issue_date DATE, expiry_date DATE, status STRING, contractor STRING",
            "id + {offset} AS permit_id, floor(rand()*5000000)+1 AS property_id, element_at(array('new_construction','renovation','addition','demolition','electrical','plumbing','roofing'),cast(floor(rand()*7)+1 as INT)) AS permit_type, concat('Permit for ',element_at(array('kitchen remodel','bathroom renovation','deck addition','pool installation','roof replacement','foundation repair'),cast(floor(rand()*6)+1 as INT))) AS description, round(rand()*500000+1000,2) AS estimated_cost, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS issue_date, date_add('2021-01-01',cast(floor(rand()*1825) as INT)) AS expiry_date, element_at(array('approved','pending','completed','expired','denied'),cast(floor(rand()*5)+1 as INT)) AS status, concat('Contractor-',floor(rand()*500)+1) AS contractor"),
        _quick_table("comparables", 100_000, "comp_id BIGINT, subject_property_id BIGINT, comp_property_id BIGINT, distance_miles DOUBLE, price_diff_pct DECIMAL(5,2), sqft_diff INT, sale_date DATE, adjustment DECIMAL(10,2)",
            "id + {offset} AS comp_id, floor(rand()*5000000)+1 AS subject_property_id, floor(rand()*5000000)+1 AS comp_property_id, round(rand()*5+0.1,2) AS distance_miles, round(rand()*40-20,2) AS price_diff_pct, cast(floor(rand()*2000)-1000 as INT) AS sqft_diff, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS sale_date, round(rand()*50000-25000,2) AS adjustment"),
    ],
    "views": [
        ("v_market_summary", "SELECT p.city, p.state, count(l.listing_id) AS listings, avg(l.list_price) AS avg_price, avg(l.sqft) AS avg_sqft FROM {s}.listings l JOIN {s}.properties p ON l.property_id = p.property_id GROUP BY p.city, p.state"),
        ("v_agent_performance", "SELECT a.first_name, a.last_name, a.brokerage, count(t.txn_id) AS sales, sum(t.sale_price) AS total_volume FROM {s}.agents a LEFT JOIN {s}.transactions_re t ON a.agent_id = t.buyer_id OR a.agent_id = t.seller_id GROUP BY a.first_name, a.last_name, a.brokerage"),
        ("v_price_trends", "SELECT mt.zip_code, mt.month, mt.median_price, mt.avg_days_on_market FROM {s}.market_trends mt ORDER BY mt.zip_code, mt.month"),
        ("v_listing_funnel", "SELECT l.status, count(*) AS listings, avg(l.list_price) AS avg_price FROM {s}.listings l GROUP BY l.status"),
        ("v_property_by_type", "SELECT property_type, count(*) AS properties, avg(assessed_value) AS avg_value FROM {s}.properties GROUP BY property_type"),
        ("v_mortgage_portfolio", "SELECT lender, loan_type, count(*) AS loans, avg(interest_rate) AS avg_rate, sum(loan_amount) AS total FROM {s}.mortgages GROUP BY lender, loan_type"),
        ("v_rental_collection", "SELECT status, count(*) AS payments, sum(amount) AS total, sum(late_fee) AS total_fees FROM {s}.rental_payments GROUP BY status"),
        ("v_neighborhood_rankings", "SELECT name, city, median_price, school_rating, walkability_score FROM {s}.neighborhoods ORDER BY median_price DESC"),
        ("v_open_house_conversion", "SELECT oh.listing_id, count(*) AS open_houses, sum(oh.attendees) AS total_visitors, sum(oh.offers_received) AS total_offers FROM {s}.open_houses oh GROUP BY oh.listing_id"),
        ("v_tax_assessment", "SELECT tax_year, count(*) AS properties, avg(assessed_value) AS avg_assessed, avg(tax_amount) AS avg_tax FROM {s}.property_taxes GROUP BY tax_year"),
        ("v_construction_activity", "SELECT permit_type, status, count(*) AS permits, sum(estimated_cost) AS total_value FROM {s}.construction_permits GROUP BY permit_type, status"),
        ("v_appraisal_accuracy", "SELECT purpose, avg(abs(a.appraised_value - l.list_price)/l.list_price*100) AS avg_diff_pct FROM {s}.appraisals a JOIN {s}.listings l ON a.property_id = l.property_id GROUP BY purpose"),
        ("v_inspection_issues", "SELECT inspection_type, avg(issues_found) AS avg_issues, avg(estimated_repair_cost) AS avg_repair_cost FROM {s}.inspections GROUP BY inspection_type"),
        ("v_hoa_delinquency", "SELECT community, status, count(*) AS units, sum(monthly_dues) AS monthly_revenue FROM {s}.hoa_dues GROUP BY community, status"),
        ("v_days_on_market", "SELECT l.property_type, avg(t.days_on_market) AS avg_dom, min(t.days_on_market) AS min_dom, max(t.days_on_market) AS max_dom FROM {s}.transactions_re t JOIN {s}.listings l ON t.listing_id = l.listing_id GROUP BY l.property_type"),
        ("v_comp_analysis", "SELECT subject_property_id, count(*) AS comps_used, avg(price_diff_pct) AS avg_price_diff, avg(distance_miles) AS avg_distance FROM {s}.comparables GROUP BY subject_property_id"),
        ("v_monthly_closings", "SELECT date_trunc('month',closing_date) AS month, count(*) AS sales, avg(sale_price) AS avg_price FROM {s}.transactions_re GROUP BY date_trunc('month',closing_date)"),
        ("v_price_per_sqft", "SELECT l.property_type, avg(l.list_price/l.sqft) AS avg_price_per_sqft FROM {s}.listings l WHERE l.sqft > 0 GROUP BY l.property_type"),
        ("v_viewing_conversion", "SELECT view_type, source, count(*) AS views, sum(CASE WHEN inquired THEN 1 ELSE 0 END) AS inquiries FROM {s}.property_views GROUP BY view_type, source"),
        ("v_brokerage_market_share", "SELECT a.brokerage, count(DISTINCT a.agent_id) AS agents, count(t.txn_id) AS transactions FROM {s}.agents a LEFT JOIN {s}.transactions_re t ON a.agent_id = t.buyer_id GROUP BY a.brokerage"),
    ],
    "udfs": [
        ("format_price_re", "price DECIMAL(12,2)", "STRING", "Format real estate price", "CASE WHEN price >= 1000000 THEN concat('$',round(price/1000000,2),'M') WHEN price >= 1000 THEN concat('$',round(price/1000,0),'K') ELSE concat('$',format_number(price,0)) END"),
        ("price_per_sqft", "price DECIMAL(12,2), sqft INT", "DECIMAL(8,2)", "Price per square foot", "round(price / GREATEST(sqft, 1), 2)"),
        ("mortgage_monthly", "principal DECIMAL(12,2), annual_rate DECIMAL(5,4), term_years INT", "DECIMAL(10,2)", "Monthly mortgage payment", "round(principal * (annual_rate/12) / (1 - power(1 + annual_rate/12, -term_years*12)), 2)"),
        ("cap_rate", "noi DECIMAL(10,2), value DECIMAL(12,2)", "DECIMAL(5,2)", "Capitalization rate", "round(noi / GREATEST(value, 1) * 100, 2)"),
        ("property_age", "year_built INT", "INT", "Property age in years", "year(current_date()) - year_built"),
        ("market_status", "days_on_market INT", "STRING", "Market status", "CASE WHEN days_on_market < 7 THEN 'Hot' WHEN days_on_market < 30 THEN 'Active' WHEN days_on_market < 90 THEN 'Slow' ELSE 'Stale' END"),
        ("listing_status_label", "status STRING", "STRING", "Status label", "initcap(replace(status,'_',' '))"),
        ("tax_estimate", "value DECIMAL(12,2), rate DECIMAL(6,4)", "DECIMAL(10,2)", "Estimated annual tax", "round(value * rate, 2)"),
        ("roi_rental", "monthly_rent DECIMAL(8,2), purchase_price DECIMAL(12,2)", "DECIMAL(5,2)", "Gross rental ROI %", "round(monthly_rent * 12 / GREATEST(purchase_price, 1) * 100, 2)"),
        ("school_rating_label", "rating INT", "STRING", "School rating label", "CASE WHEN rating >= 9 THEN 'Excellent' WHEN rating >= 7 THEN 'Good' WHEN rating >= 5 THEN 'Average' ELSE 'Below Average' END"),
        ("mask_address", "address STRING", "STRING", "Masks street address", "concat(left(address,4),'*** ',split(address,' ')[size(split(address,' '))-1])"),
        ("down_payment", "price DECIMAL(12,2), pct DECIMAL(5,2)", "DECIMAL(12,2)", "Down payment amount", "round(price * pct / 100, 2)"),
        ("commission_split", "sale_price DECIMAL(12,2), rate DECIMAL(5,2)", "DECIMAL(10,2)", "Agent commission", "round(sale_price * rate / 100, 2)"),
        ("is_luxury", "price DECIMAL(12,2)", "BOOLEAN", "Luxury property check", "price >= 1000000"),
        ("walkability_label", "score INT", "STRING", "Walkability label", "CASE WHEN score >= 90 THEN 'Walkers Paradise' WHEN score >= 70 THEN 'Very Walkable' WHEN score >= 50 THEN 'Somewhat Walkable' ELSE 'Car-Dependent' END"),
        ("appreciation_estimate", "current_value DECIMAL(12,2), annual_rate DECIMAL(5,4), years INT", "DECIMAL(12,2)", "Estimated future value", "round(current_value * power(1 + annual_rate, years), 2)"),
        ("ltv_ratio", "loan DECIMAL(12,2), value DECIMAL(12,2)", "DECIMAL(5,2)", "Loan-to-value ratio", "round(loan / GREATEST(value, 1) * 100, 2)"),
        ("inspection_grade", "issues INT", "STRING", "Inspection grade", "CASE WHEN issues = 0 THEN 'A' WHEN issues <= 3 THEN 'B' WHEN issues <= 7 THEN 'C' WHEN issues <= 15 THEN 'D' ELSE 'F' END"),
        ("hoa_status_label", "status STRING", "STRING", "HOA payment status", "initcap(replace(status,'_',' '))"),
        ("anonymize_property", "property_id BIGINT", "STRING", "Anonymized property ref", "concat('PROP-',substring(sha2(cast(property_id as STRING),256),1,8))"),
    ],
}

INDUSTRIES["logistics"] = {
    "tables": [
        _quick_table("shipments", 100_000_000, "shipment_id BIGINT, order_id BIGINT, origin STRING, destination STRING, carrier STRING, weight_kg DECIMAL(10,2), volume_m3 DECIMAL(8,2), ship_date DATE, delivery_date DATE, status STRING",
            "id + {offset} AS shipment_id, floor(rand()*20000000)+1 AS order_id, element_at(array('Shanghai','Rotterdam','LA','Singapore','Dubai','Hamburg','New York','Shenzhen','Busan','Mumbai'),cast(floor(rand()*10)+1 as INT)) AS origin, element_at(array('New York','Chicago','Dallas','Miami','LA','Seattle','Atlanta','Denver','Phoenix','Boston'),cast(floor(rand()*10)+1 as INT)) AS destination, element_at(array('FedEx','UPS','DHL','Maersk','MSC','COSCO','CMA CGM','Hapag-Lloyd','XPO','JB Hunt'),cast(floor(rand()*10)+1 as INT)) AS carrier, round(rand()*10000+0.5,2) AS weight_kg, round(rand()*50+0.1,2) AS volume_m3, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS ship_date, date_add('2020-01-01',cast(floor(rand()*1825)+7 as INT)) AS delivery_date, element_at(array('in_transit','delivered','customs','delayed','returned','lost'),cast(floor(rand()*6)+1 as INT)) AS status"),
        _quick_table("tracking_events", 50_000_000, "event_id BIGINT, shipment_id BIGINT, event_type STRING, location STRING, event_timestamp TIMESTAMP, description STRING, latitude DOUBLE, longitude DOUBLE, facility_id BIGINT, handler STRING",
            "id + {offset} AS event_id, floor(rand()*100000000)+1 AS shipment_id, element_at(array('pickup','departure','arrival','customs_entry','customs_clear','sorting','out_for_delivery','delivered','exception','return'),cast(floor(rand()*10)+1 as INT)) AS event_type, element_at(array('Warehouse A','Port Terminal','Hub Center','Distribution Center','Customs Office','Local Depot','Customer Address'),cast(floor(rand()*7)+1 as INT)) AS location, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS event_timestamp, concat('Package ',element_at(array('scanned','processed','loaded','unloaded','cleared','sorted','dispatched'),cast(floor(rand()*7)+1 as INT))) AS description, round(rand()*50+25,6) AS latitude, round(rand()*60-100,6) AS longitude, floor(rand()*5000)+1 AS facility_id, concat('Handler-',floor(rand()*1000)+1) AS handler"),
        _quick_table("fleet_telemetry", 30_000_000, "telemetry_id BIGINT, vehicle_id BIGINT, latitude DOUBLE, longitude DOUBLE, speed_mph DOUBLE, fuel_level_pct DECIMAL(5,2), engine_temp_f DOUBLE, odometer_miles BIGINT, timestamp_utc TIMESTAMP, idle_minutes INT",
            "id + {offset} AS telemetry_id, floor(rand()*50000)+1 AS vehicle_id, round(rand()*25+25,6) AS latitude, round(rand()*55-125,6) AS longitude, round(rand()*75,1) AS speed_mph, round(rand()*100,2) AS fuel_level_pct, round(rand()*60+160,1) AS engine_temp_f, cast(floor(rand()*500000)+1000 as BIGINT) AS odometer_miles, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS timestamp_utc, cast(floor(rand()*30) as INT) AS idle_minutes"),
        _quick_table("warehouse_inventory", 5_000_000, "inventory_id BIGINT, warehouse_id BIGINT, sku STRING, quantity INT, location_code STRING, received_date DATE, expiry_date DATE, lot_number STRING, status STRING, weight_kg DECIMAL(8,2)",
            "id + {offset} AS inventory_id, floor(rand()*500)+1 AS warehouse_id, concat('SKU-',lpad(cast(floor(rand()*999999) as STRING),6,'0')) AS sku, cast(floor(rand()*10000)+1 as INT) AS quantity, concat(element_at(array('A','B','C','D','E'),cast(floor(rand()*5)+1 as INT)),'-',lpad(cast(floor(rand()*99) as STRING),2,'0'),'-',lpad(cast(floor(rand()*20) as STRING),2,'0')) AS location_code, date_add('2023-01-01',cast(floor(rand()*730) as INT)) AS received_date, date_add('2025-01-01',cast(floor(rand()*730) as INT)) AS expiry_date, concat('LOT-',upper(substring(sha2(cast(id as STRING),256),1,8))) AS lot_number, element_at(array('available','reserved','damaged','expired','in_transit'),cast(floor(rand()*5)+1 as INT)) AS status, round(rand()*100+0.1,2) AS weight_kg"),
        _quick_table("route_plans", 5_000_000, "route_id BIGINT, vehicle_id BIGINT, driver_id BIGINT, planned_date DATE, stops INT, total_distance_miles DOUBLE, estimated_hours DOUBLE, actual_hours DOUBLE, fuel_cost DECIMAL(8,2), status STRING",
            "id + {offset} AS route_id, floor(rand()*50000)+1 AS vehicle_id, floor(rand()*20000)+1 AS driver_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS planned_date, cast(floor(rand()*20)+2 as INT) AS stops, round(rand()*500+10,2) AS total_distance_miles, round(rand()*12+1,2) AS estimated_hours, round(rand()*14+1,2) AS actual_hours, round(rand()*200+20,2) AS fuel_cost, element_at(array('completed','in_progress','planned','cancelled'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("vehicles", 1_000_000, "vehicle_id BIGINT, plate STRING, vehicle_type STRING, make STRING, model STRING, year INT, capacity_kg DECIMAL(10,2), fuel_type STRING, status STRING, last_service_date DATE",
            "id + {offset} AS vehicle_id, upper(substring(sha2(cast(id as STRING),256),1,7)) AS plate, element_at(array('semi_truck','box_truck','van','sprinter','flatbed','tanker','reefer','container'),cast(floor(rand()*8)+1 as INT)) AS vehicle_type, element_at(array('Freightliner','Peterbilt','Kenworth','Volvo','Mack','International','Mercedes','Ford','Ram','Isuzu'),cast(floor(rand()*10)+1 as INT)) AS make, concat('Model-',upper(substring(sha2(cast(id+100 as STRING),256),1,4))) AS model, cast(floor(rand()*10)+2015 as INT) AS year, round(rand()*40000+1000,2) AS capacity_kg, element_at(array('diesel','electric','cng','hybrid','gasoline'),cast(floor(rand()*5)+1 as INT)) AS fuel_type, element_at(array('active','maintenance','retired','available'),cast(floor(rand()*4)+1 as INT)) AS status, date_add('2023-01-01',cast(floor(rand()*730) as INT)) AS last_service_date"),
        _quick_table("drivers", 1_000_000, "driver_id BIGINT, first_name STRING, last_name STRING, license_class STRING, hire_date DATE, status STRING, total_miles BIGINT, safety_score DECIMAL(5,2), home_base STRING, certifications STRING",
            "id + {offset} AS driver_id, element_at(array('Mike','John','David','Carlos','James','Robert','William','Antonio','Luis','Kevin'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Garcia','Rodriguez','Martinez','Lopez','Hernandez','Davis'),cast(floor(rand()*10)+1 as INT)) AS last_name, element_at(array('CDL-A','CDL-B','CDL-C','Class D'),cast(floor(rand()*4)+1 as INT)) AS license_class, date_add('2010-01-01',cast(floor(rand()*5475) as INT)) AS hire_date, element_at(array('active','on_leave','terminated','training'),cast(floor(rand()*4)+1 as INT)) AS status, cast(floor(rand()*1000000)+10000 as BIGINT) AS total_miles, round(rand()*40+60,2) AS safety_score, element_at(array('Dallas','Chicago','Atlanta','LA','Memphis','Louisville'),cast(floor(rand()*6)+1 as INT)) AS home_base, element_at(array('HAZMAT','tanker','doubles','passenger','none'),cast(floor(rand()*5)+1 as INT)) AS certifications"),
        _quick_table("warehouses_lg", 1_000_000, "warehouse_id BIGINT, name STRING, address STRING, city STRING, state STRING, sqft INT, warehouse_type STRING, temperature_controlled BOOLEAN, dock_doors INT, status STRING",
            "id + {offset} AS warehouse_id, concat(element_at(array('Alpha','Beta','Gamma','Delta','Omega','Prime','Central','Metro','Global','Express'),cast(floor(rand()*10)+1 as INT)),' DC-',id) AS name, concat(cast(floor(rand()*9999)+1 as STRING),' Logistics Pkwy') AS address, element_at(array('Memphis','Dallas','Chicago','Louisville','Indianapolis','Columbus','Atlanta','Nashville','Kansas City','Phoenix'),cast(floor(rand()*10)+1 as INT)) AS city, element_at(array('TN','TX','IL','KY','IN','OH','GA','TN','MO','AZ'),cast(floor(rand()*10)+1 as INT)) AS state, cast(floor(rand()*1000000)+10000 as INT) AS sqft, element_at(array('distribution','fulfillment','cross_dock','cold_storage','bulk'),cast(floor(rand()*5)+1 as INT)) AS warehouse_type, rand() > 0.7 AS temperature_controlled, cast(floor(rand()*50)+2 as INT) AS dock_doors, element_at(array('operational','construction','maintenance','closed'),cast(floor(rand()*4)+1 as INT)) AS status"),
        _quick_table("customs_entries", 500_000, "entry_id BIGINT, shipment_id BIGINT, entry_type STRING, hs_code STRING, declared_value DECIMAL(12,2), duty_amount DECIMAL(10,2), origin_country STRING, dest_country STRING, filing_date DATE, clearance_date DATE",
            "id + {offset} AS entry_id, floor(rand()*100000000)+1 AS shipment_id, element_at(array('import','export','transit','temporary'),cast(floor(rand()*4)+1 as INT)) AS entry_type, lpad(cast(floor(rand()*999999) as STRING),6,'0') AS hs_code, round(rand()*500000+100,2) AS declared_value, round(rand()*50000+10,2) AS duty_amount, element_at(array('CN','DE','JP','KR','TW','MX','IN','VN','UK','IT'),cast(floor(rand()*10)+1 as INT)) AS origin_country, 'US' AS dest_country, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS filing_date, date_add('2020-01-01',cast(floor(rand()*1825)+3 as INT)) AS clearance_date"),
        _quick_table("freight_rates", 500_000, "rate_id BIGINT, origin STRING, destination STRING, carrier STRING, mode STRING, rate_per_kg DECIMAL(8,4), rate_per_m3 DECIMAL(8,4), min_charge DECIMAL(8,2), effective_date DATE, currency STRING",
            "id + {offset} AS rate_id, element_at(array('Shanghai','Rotterdam','LA','Singapore','Dubai'),cast(floor(rand()*5)+1 as INT)) AS origin, element_at(array('New York','Chicago','Dallas','Miami','LA'),cast(floor(rand()*5)+1 as INT)) AS destination, element_at(array('FedEx','UPS','DHL','Maersk','MSC'),cast(floor(rand()*5)+1 as INT)) AS carrier, element_at(array('air','ocean_fcl','ocean_lcl','rail','truck','intermodal'),cast(floor(rand()*6)+1 as INT)) AS mode, round(rand()*5+0.1,4) AS rate_per_kg, round(rand()*200+10,4) AS rate_per_m3, round(rand()*100+10,2) AS min_charge, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date, 'USD' AS currency"),
        _quick_table("delivery_exceptions", 300_000, "exception_id BIGINT, shipment_id BIGINT, exception_type STRING, description STRING, reported_at TIMESTAMP, resolved_at TIMESTAMP, impact STRING, resolution STRING",
            "id + {offset} AS exception_id, floor(rand()*100000000)+1 AS shipment_id, element_at(array('delay','damage','wrong_address','refused','customs_hold','weather','missed_pickup','capacity'),cast(floor(rand()*8)+1 as INT)) AS exception_type, concat('Exception: ',element_at(array('package delayed','damaged in transit','address incorrect','delivery refused','held at customs','weather delay'),cast(floor(rand()*6)+1 as INT))) AS description, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS reported_at, dateadd(SECOND, cast(floor(rand()*1825*86400)+86400 as INT), '2020-01-01') AS resolved_at, element_at(array('minor','moderate','severe','critical'),cast(floor(rand()*4)+1 as INT)) AS impact, element_at(array('rerouted','reshipped','refunded','returned','resolved'),cast(floor(rand()*5)+1 as INT)) AS resolution"),
        _quick_table("fuel_purchases", 300_000, "purchase_id BIGINT, vehicle_id BIGINT, fuel_type STRING, gallons DECIMAL(8,2), price_per_gallon DECIMAL(6,4), total_cost DECIMAL(8,2), station STRING, purchase_date DATE, odometer BIGINT",
            "id + {offset} AS purchase_id, floor(rand()*50000)+1 AS vehicle_id, element_at(array('diesel','gasoline','cng','def'),cast(floor(rand()*4)+1 as INT)) AS fuel_type, round(rand()*200+10,2) AS gallons, round(rand()*3+2,4) AS price_per_gallon, round(rand()*600+20,2) AS total_cost, element_at(array('Pilot','Loves','TA','Flying J','Shell','BP'),cast(floor(rand()*6)+1 as INT)) AS station, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS purchase_date, cast(floor(rand()*500000)+10000 as BIGINT) AS odometer"),
        _quick_table("dock_schedules", 200_000, "schedule_id BIGINT, warehouse_id BIGINT, dock_number INT, carrier STRING, scheduled_time TIMESTAMP, actual_time TIMESTAMP, load_type STRING, status STRING, duration_minutes INT",
            "id + {offset} AS schedule_id, floor(rand()*500)+1 AS warehouse_id, cast(floor(rand()*50)+1 as INT) AS dock_number, element_at(array('FedEx','UPS','DHL','XPO','JB Hunt'),cast(floor(rand()*5)+1 as INT)) AS carrier, dateadd(SECOND, cast(floor(rand()*1825*86400) as INT), '2020-01-01') AS scheduled_time, dateadd(SECOND, cast(floor(rand()*1825*86400)+3600 as INT), '2020-01-01') AS actual_time, element_at(array('inbound','outbound','cross_dock','return'),cast(floor(rand()*4)+1 as INT)) AS load_type, element_at(array('completed','in_progress','scheduled','delayed','cancelled'),cast(floor(rand()*5)+1 as INT)) AS status, cast(floor(rand()*120)+15 as INT) AS duration_minutes"),
        _quick_table("claims_lg", 200_000, "claim_id BIGINT, shipment_id BIGINT, claim_type STRING, amount DECIMAL(10,2), filed_date DATE, resolved_date DATE, status STRING, evidence STRING, carrier_liable BOOLEAN",
            "id + {offset} AS claim_id, floor(rand()*100000000)+1 AS shipment_id, element_at(array('damage','loss','delay','shortage','contamination','theft'),cast(floor(rand()*6)+1 as INT)) AS claim_type, round(rand()*50000+100,2) AS amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS filed_date, date_add('2020-01-01',cast(floor(rand()*1825)+30 as INT)) AS resolved_date, element_at(array('filed','under_review','approved','denied','settled'),cast(floor(rand()*5)+1 as INT)) AS status, element_at(array('photos','inspection_report','bol','packing_list'),cast(floor(rand()*4)+1 as INT)) AS evidence, rand() > 0.4 AS carrier_liable"),
        _quick_table("packaging", 100_000, "package_id BIGINT, shipment_id BIGINT, package_type STRING, length_cm DOUBLE, width_cm DOUBLE, height_cm DOUBLE, weight_kg DECIMAL(8,2), is_fragile BOOLEAN, is_hazmat BOOLEAN",
            "id + {offset} AS package_id, floor(rand()*100000000)+1 AS shipment_id, element_at(array('box','pallet','crate','drum','envelope','tube','bag'),cast(floor(rand()*7)+1 as INT)) AS package_type, round(rand()*200+5,1) AS length_cm, round(rand()*200+5,1) AS width_cm, round(rand()*200+5,1) AS height_cm, round(rand()*500+0.1,2) AS weight_kg, rand() > 0.8 AS is_fragile, rand() > 0.95 AS is_hazmat"),
        _quick_table("returns_lg", 100_000, "return_id BIGINT, shipment_id BIGINT, reason STRING, return_date DATE, condition STRING, refund_amount DECIMAL(10,2), restocking_fee DECIMAL(8,2), status STRING",
            "id + {offset} AS return_id, floor(rand()*100000000)+1 AS shipment_id, element_at(array('defective','wrong_item','damaged','changed_mind','duplicate','not_needed'),cast(floor(rand()*6)+1 as INT)) AS reason, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS return_date, element_at(array('new','opened','damaged','used'),cast(floor(rand()*4)+1 as INT)) AS condition, round(rand()*5000+10,2) AS refund_amount, round(rand()*50,2) AS restocking_fee, element_at(array('received','inspected','refunded','restocked','disposed'),cast(floor(rand()*5)+1 as INT)) AS status"),
    ],
    "views": [
        ("v_shipment_summary", "SELECT carrier, status, count(*) AS shipments, avg(weight_kg) AS avg_weight FROM {s}.shipments GROUP BY carrier, status"),
        ("v_on_time_delivery", "SELECT carrier, count(*) AS total, sum(CASE WHEN delivery_date <= date_add(ship_date,7) THEN 1 ELSE 0 END) AS on_time FROM {s}.shipments WHERE status='delivered' GROUP BY carrier"),
        ("v_fleet_utilization", "SELECT v.vehicle_type, v.status, count(*) AS vehicles, avg(v.capacity_kg) AS avg_capacity FROM {s}.vehicles v GROUP BY v.vehicle_type, v.status"),
        ("v_warehouse_capacity", "SELECT w.name, w.city, w.sqft, w.dock_doors, count(wi.inventory_id) AS items FROM {s}.warehouses_lg w LEFT JOIN {s}.warehouse_inventory wi ON w.warehouse_id = wi.warehouse_id GROUP BY w.name, w.city, w.sqft, w.dock_doors"),
        ("v_route_efficiency", "SELECT driver_id, count(*) AS routes, avg(total_distance_miles) AS avg_miles, avg(actual_hours/estimated_hours*100) AS efficiency_pct FROM {s}.route_plans WHERE status='completed' GROUP BY driver_id"),
        ("v_exception_analysis", "SELECT exception_type, impact, count(*) AS exceptions FROM {s}.delivery_exceptions GROUP BY exception_type, impact"),
        ("v_customs_processing", "SELECT origin_country, avg(datediff(clearance_date,filing_date)) AS avg_clearance_days, count(*) AS entries FROM {s}.customs_entries GROUP BY origin_country"),
        ("v_fuel_costs", "SELECT v.vehicle_type, avg(fp.price_per_gallon) AS avg_price, sum(fp.total_cost) AS total_fuel_cost FROM {s}.fuel_purchases fp JOIN {s}.vehicles v ON fp.vehicle_id = v.vehicle_id GROUP BY v.vehicle_type"),
        ("v_driver_safety", "SELECT d.first_name, d.last_name, d.safety_score, d.certifications, d.total_miles FROM {s}.drivers d WHERE d.status='active' ORDER BY d.safety_score DESC"),
        ("v_rate_comparison", "SELECT origin, destination, mode, avg(rate_per_kg) AS avg_rate FROM {s}.freight_rates GROUP BY origin, destination, mode"),
        ("v_tracking_timeline", "SELECT event_type, count(*) AS events, avg(CASE WHEN event_type='delivered' THEN 1 ELSE 0 END) AS delivery_rate FROM {s}.tracking_events GROUP BY event_type"),
        ("v_dock_utilization", "SELECT warehouse_id, count(*) AS schedules, avg(duration_minutes) AS avg_duration FROM {s}.dock_schedules WHERE status='completed' GROUP BY warehouse_id"),
        ("v_claims_overview", "SELECT claim_type, status, count(*) AS claims, sum(amount) AS total_amount FROM {s}.claims_lg GROUP BY claim_type, status"),
        ("v_inventory_aging", "SELECT warehouse_id, status, count(*) AS items, avg(datediff(current_date(),received_date)) AS avg_age_days FROM {s}.warehouse_inventory GROUP BY warehouse_id, status"),
        ("v_package_analysis", "SELECT package_type, count(*) AS packages, avg(weight_kg) AS avg_weight, sum(CASE WHEN is_fragile THEN 1 ELSE 0 END) AS fragile_count FROM {s}.packaging GROUP BY package_type"),
        ("v_return_reasons", "SELECT reason, condition, count(*) AS returns, sum(refund_amount) AS total_refunded FROM {s}.returns_lg GROUP BY reason, condition"),
        ("v_telemetry_alerts", "SELECT vehicle_id, count(*) AS readings, avg(speed_mph) AS avg_speed, avg(fuel_level_pct) AS avg_fuel FROM {s}.fleet_telemetry WHERE speed_mph > 70 OR fuel_level_pct < 20 GROUP BY vehicle_id"),
        ("v_monthly_volume", "SELECT date_trunc('month',ship_date) AS month, carrier, count(*) AS shipments FROM {s}.shipments GROUP BY date_trunc('month',ship_date), carrier"),
        ("v_lane_analysis", "SELECT origin, destination, count(*) AS shipments, avg(weight_kg) AS avg_weight FROM {s}.shipments GROUP BY origin, destination"),
        ("v_fleet_maintenance", "SELECT vehicle_type, avg(datediff(current_date(),last_service_date)) AS avg_days_since_service FROM {s}.vehicles GROUP BY vehicle_type"),
    ],
    "udfs": [
        ("format_weight", "kg DECIMAL(10,2)", "STRING", "Format weight", "CASE WHEN kg >= 1000 THEN concat(round(kg/1000,2),' T') ELSE concat(round(kg,1),' kg') END"),
        ("format_distance", "miles DOUBLE", "STRING", "Format distance", "CASE WHEN miles >= 1000 THEN concat(round(miles/1000,1),'K mi') ELSE concat(round(miles,0),' mi') END"),
        ("delivery_status_label", "status STRING", "STRING", "Status label", "initcap(replace(status,'_',' '))"),
        ("transit_time_sla", "ship_date DATE, delivery_date DATE, sla_days INT", "STRING", "SLA compliance", "CASE WHEN datediff(delivery_date,ship_date) <= sla_days THEN 'Met' ELSE 'Breached' END"),
        ("dim_weight", "l DOUBLE, w DOUBLE, h DOUBLE", "DECIMAL(8,2)", "Dimensional weight (kg)", "round(l*w*h/5000, 2)"),
        ("fuel_efficiency", "miles DOUBLE, gallons DECIMAL(8,2)", "DECIMAL(5,2)", "Miles per gallon", "round(miles/GREATEST(gallons,0.01), 2)"),
        ("driver_hours_remaining", "worked DOUBLE", "DECIMAL(4,1)", "HOS remaining hours", "round(GREATEST(11-worked, 0), 1)"),
        ("mask_plate", "plate STRING", "STRING", "Masks license plate", "concat('***',right(plate,3))"),
        ("is_overweight", "weight_kg DECIMAL(10,2), capacity_kg DECIMAL(10,2)", "BOOLEAN", "Overweight check", "weight_kg > capacity_kg"),
        ("customs_duty_rate", "hs_code STRING", "DECIMAL(5,2)", "Estimated duty rate", "CASE WHEN left(hs_code,2) IN ('01','02','03','04') THEN 5.0 WHEN left(hs_code,2) IN ('84','85') THEN 2.5 ELSE 3.5 END"),
        ("exception_priority", "impact STRING", "INT", "Exception priority", "CASE WHEN impact = 'critical' THEN 1 WHEN impact = 'severe' THEN 2 WHEN impact = 'moderate' THEN 3 ELSE 4 END"),
        ("warehouse_zone", "location_code STRING", "STRING", "Warehouse zone from location", "left(location_code,1)"),
        ("cargo_class", "is_fragile BOOLEAN, is_hazmat BOOLEAN", "STRING", "Cargo classification", "CASE WHEN is_hazmat THEN 'Hazmat' WHEN is_fragile THEN 'Fragile' ELSE 'Standard' END"),
        ("rate_per_mile", "total_cost DECIMAL(8,2), miles DOUBLE", "DECIMAL(6,4)", "Cost per mile", "round(total_cost/GREATEST(miles,1), 4)"),
        ("anonymize_shipment", "shipment_id BIGINT", "STRING", "Anonymized shipment ref", "concat('SHP-',substring(sha2(cast(shipment_id as STRING),256),1,8))"),
        ("claim_severity", "amount DECIMAL(10,2)", "STRING", "Claim severity", "CASE WHEN amount > 10000 THEN 'Major' WHEN amount > 1000 THEN 'Moderate' ELSE 'Minor' END"),
        ("dock_utilization_pct", "scheduled INT, capacity INT", "DECIMAL(5,2)", "Dock utilization %", "round(scheduled*100.0/GREATEST(capacity,1), 2)"),
        ("package_volume_m3", "l DOUBLE, w DOUBLE, h DOUBLE", "DECIMAL(8,4)", "Volume in cubic meters", "round(l*w*h/1000000, 4)"),
        ("eta_status", "estimated TIMESTAMP, actual TIMESTAMP", "STRING", "ETA vs actual", "CASE WHEN actual IS NULL THEN 'Pending' WHEN actual <= estimated THEN 'On Time' ELSE 'Late' END"),
        ("format_tracking_id", "id BIGINT", "STRING", "Formatted tracking ID", "concat('TRK-',lpad(cast(id as STRING),12,'0'))"),
    ],
}

INDUSTRIES["insurance"] = {
    "tables": [
        _quick_table("policies", 100_000_000, "policy_id BIGINT, customer_id BIGINT, policy_type STRING, effective_date DATE, expiry_date DATE, premium DECIMAL(10,2), deductible DECIMAL(10,2), coverage_limit DECIMAL(14,2), status STRING, agent_id BIGINT",
            "id + {offset} AS policy_id, floor(rand()*5000000)+1 AS customer_id, element_at(array('auto','home','life','health','business','umbrella','renters','pet','travel','cyber'),cast(floor(rand()*10)+1 as INT)) AS policy_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date, date_add('2021-01-01',cast(floor(rand()*1825) as INT)) AS expiry_date, round(rand()*5000+100,2) AS premium, round(rand()*5000+250,2) AS deductible, round(rand()*2000000+10000,2) AS coverage_limit, element_at(array('active','expired','cancelled','pending','lapsed'),cast(floor(rand()*5)+1 as INT)) AS status, floor(rand()*50000)+1 AS agent_id"),
        _quick_table("claims_ins", 50_000_000, "claim_id BIGINT, policy_id BIGINT, claim_date DATE, loss_date DATE, claim_type STRING, amount_claimed DECIMAL(12,2), amount_paid DECIMAL(12,2), status STRING, adjuster_id BIGINT, description STRING",
            "id + {offset} AS claim_id, floor(rand()*100000000)+1 AS policy_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS claim_date, date_add('2020-01-01',cast(floor(rand()*1825)-5 as INT)) AS loss_date, element_at(array('collision','theft','fire','water','liability','medical','property','glass','comprehensive','fraud'),cast(floor(rand()*10)+1 as INT)) AS claim_type, round(rand()*100000+100,2) AS amount_claimed, round(rand()*80000+50,2) AS amount_paid, element_at(array('filed','under_review','approved','denied','settled','closed','reopened'),cast(floor(rand()*7)+1 as INT)) AS status, floor(rand()*5000)+1 AS adjuster_id, concat('Claim for ',element_at(array('vehicle damage','property damage','theft','injury','liability','natural disaster'),cast(floor(rand()*6)+1 as INT))) AS description"),
        _quick_table("underwriting", 30_000_000, "underwriting_id BIGINT, policy_id BIGINT, risk_score DOUBLE, risk_class STRING, factors STRING, decision STRING, premium_modifier DECIMAL(5,2), underwriter_id BIGINT, decision_date DATE, model_version STRING",
            "id + {offset} AS underwriting_id, floor(rand()*100000000)+1 AS policy_id, round(rand()*100,1) AS risk_score, element_at(array('preferred','standard','substandard','declined'),cast(floor(rand()*4)+1 as INT)) AS risk_class, element_at(array('age','driving_record','credit_score','claims_history','location','property_age','health','occupation'),cast(floor(rand()*8)+1 as INT)) AS factors, element_at(array('approved','declined','referred','conditional'),cast(floor(rand()*4)+1 as INT)) AS decision, round(rand()*2-0.5,2) AS premium_modifier, floor(rand()*500)+1 AS underwriter_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS decision_date, element_at(array('v1.0','v2.0','v3.0','v3.1'),cast(floor(rand()*4)+1 as INT)) AS model_version"),
        _quick_table("policyholders", 1_000_000, "customer_id BIGINT, first_name STRING, last_name STRING, date_of_birth DATE, gender STRING, email STRING, phone STRING, address STRING, city STRING, state STRING",
            "id + {offset} AS customer_id, element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth'),cast(floor(rand()*10)+1 as INT)) AS first_name, element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez'),cast(floor(rand()*10)+1 as INT)) AS last_name, date_add('1950-01-01',cast(floor(rand()*21900) as INT)) AS date_of_birth, element_at(array('M','F'),cast(floor(rand()*2)+1 as INT)) AS gender, concat('policy',id + {offset},'@example.com') AS email, concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone, concat(cast(floor(rand()*9999)+1 as STRING),' Insurance Ln') AS address, element_at(array('New York','LA','Chicago','Houston','Phoenix'),cast(floor(rand()*5)+1 as INT)) AS city, element_at(array('NY','CA','IL','TX','AZ'),cast(floor(rand()*5)+1 as INT)) AS state"),
        _quick_table("agents_ins", 1_000_000, "agent_id BIGINT, first_name STRING, last_name STRING, agency STRING, license_state STRING, commission_rate DECIMAL(5,2), policies_sold INT, premium_volume DECIMAL(14,2), hire_date DATE, status STRING",
            "id + {offset} AS agent_id, element_at(array('Sarah','David','Emily','James','Anna'),cast(floor(rand()*5)+1 as INT)) AS first_name, element_at(array('Chen','Patel','Kim','Singh','Lee'),cast(floor(rand()*5)+1 as INT)) AS last_name, element_at(array('State Farm','Allstate','GEICO','Progressive','Liberty Mutual','Farmers','Nationwide','USAA','Travelers','Hartford'),cast(floor(rand()*10)+1 as INT)) AS agency, element_at(array('CA','NY','TX','FL','IL'),cast(floor(rand()*5)+1 as INT)) AS license_state, round(rand()*15+5,2) AS commission_rate, cast(floor(rand()*5000)+10 as INT) AS policies_sold, round(rand()*5000000+10000,2) AS premium_volume, date_add('2005-01-01',cast(floor(rand()*7300) as INT)) AS hire_date, element_at(array('active','inactive','terminated'),cast(floor(rand()*3)+1 as INT)) AS status"),
        _quick_table("payments_ins", 1_000_000, "payment_id BIGINT, policy_id BIGINT, amount DECIMAL(10,2), payment_date DATE, payment_method STRING, payment_type STRING, status STRING, period_start DATE, period_end DATE",
            "id + {offset} AS payment_id, floor(rand()*100000000)+1 AS policy_id, round(rand()*2000+50,2) AS amount, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS payment_date, element_at(array('credit_card','ach','check','payroll_deduction','online'),cast(floor(rand()*5)+1 as INT)) AS payment_method, element_at(array('premium','deductible','copay','coinsurance'),cast(floor(rand()*4)+1 as INT)) AS payment_type, element_at(array('completed','pending','failed','refunded'),cast(floor(rand()*4)+1 as INT)) AS status, date_add('2020-01-01',cast(floor(rand()*60)*30 as INT)) AS period_start, date_add('2020-01-01',cast(floor(rand()*60)*30+30 as INT)) AS period_end"),
        _quick_table("loss_runs", 1_000_000, "loss_id BIGINT, policy_id BIGINT, loss_date DATE, loss_type STRING, amount DECIMAL(12,2), reserve DECIMAL(12,2), paid DECIMAL(12,2), incurred DECIMAL(12,2), status STRING, catastrophe_code STRING",
            "id + {offset} AS loss_id, floor(rand()*100000000)+1 AS policy_id, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS loss_date, element_at(array('property','liability','medical','auto','workers_comp','professional'),cast(floor(rand()*6)+1 as INT)) AS loss_type, round(rand()*200000+100,2) AS amount, round(rand()*200000+100,2) AS reserve, round(rand()*150000+50,2) AS paid, round(rand()*250000+100,2) AS incurred, element_at(array('open','closed','reopened'),cast(floor(rand()*3)+1 as INT)) AS status, CASE WHEN rand() > 0.9 THEN concat('CAT-',floor(rand()*50)+1) ELSE NULL END AS catastrophe_code"),
        _quick_table("reinsurance", 500_000, "treaty_id BIGINT, reinsurer STRING, treaty_type STRING, retention DECIMAL(14,2), limit_amount DECIMAL(14,2), premium DECIMAL(12,2), effective_date DATE, expiry_date DATE, line_of_business STRING",
            "id + {offset} AS treaty_id, element_at(array('Munich Re','Swiss Re','Berkshire','Hannover Re','SCOR','RGA','Gen Re','Everest Re','PartnerRe','Transatlantic'),cast(floor(rand()*10)+1 as INT)) AS reinsurer, element_at(array('quota_share','surplus','excess_of_loss','catastrophe','aggregate'),cast(floor(rand()*5)+1 as INT)) AS treaty_type, round(rand()*10000000+100000,2) AS retention, round(rand()*100000000+1000000,2) AS limit_amount, round(rand()*5000000+50000,2) AS premium, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date, date_add('2021-01-01',cast(floor(rand()*1825) as INT)) AS expiry_date, element_at(array('auto','property','casualty','life','health'),cast(floor(rand()*5)+1 as INT)) AS line_of_business"),
        _quick_table("fraud_detection", 500_000, "detection_id BIGINT, claim_id BIGINT, fraud_score DOUBLE, indicators STRING, investigation_status STRING, detected_date DATE, investigator STRING, outcome STRING, savings DECIMAL(10,2)",
            "id + {offset} AS detection_id, floor(rand()*50000000)+1 AS claim_id, round(rand()*100,1) AS fraud_score, element_at(array('duplicate_claim','suspicious_timing','inflated_amount','staged_accident','identity_theft','phantom_treatment'),cast(floor(rand()*6)+1 as INT)) AS indicators, element_at(array('flagged','investigating','confirmed','cleared','referred'),cast(floor(rand()*5)+1 as INT)) AS investigation_status, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS detected_date, concat('Investigator-',floor(rand()*100)+1) AS investigator, element_at(array('fraud_confirmed','legitimate','inconclusive','settled'),cast(floor(rand()*4)+1 as INT)) AS outcome, round(rand()*100000,2) AS savings"),
        _quick_table("actuarial_tables", 500_000, "table_id BIGINT, table_type STRING, age_band STRING, gender STRING, risk_class STRING, mortality_rate DECIMAL(8,6), morbidity_rate DECIMAL(8,6), loss_ratio DECIMAL(5,2), effective_date DATE",
            "id + {offset} AS table_id, element_at(array('mortality','morbidity','loss_cost','frequency','severity'),cast(floor(rand()*5)+1 as INT)) AS table_type, concat(cast(floor(rand()*8)*10+18 as STRING),'-',cast(floor(rand()*8)*10+27 as STRING)) AS age_band, element_at(array('M','F','All'),cast(floor(rand()*3)+1 as INT)) AS gender, element_at(array('preferred','standard','substandard'),cast(floor(rand()*3)+1 as INT)) AS risk_class, round(rand()*0.05,6) AS mortality_rate, round(rand()*0.1,6) AS morbidity_rate, round(rand()*100,2) AS loss_ratio, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date"),
        _quick_table("endorsements", 300_000, "endorsement_id BIGINT, policy_id BIGINT, endorsement_type STRING, effective_date DATE, premium_change DECIMAL(8,2), description STRING, approved_by STRING",
            "id + {offset} AS endorsement_id, floor(rand()*100000000)+1 AS policy_id, element_at(array('add_coverage','remove_coverage','change_limit','change_deductible','add_driver','add_property','name_change'),cast(floor(rand()*7)+1 as INT)) AS endorsement_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS effective_date, round(rand()*500-200,2) AS premium_change, concat('Endorsement: ',element_at(array('Added roadside','Increased limit','Changed deductible','Added driver','Updated address'),cast(floor(rand()*5)+1 as INT))) AS description, concat('Underwriter-',floor(rand()*100)+1) AS approved_by"),
        _quick_table("quotes", 300_000, "quote_id BIGINT, customer_id BIGINT, policy_type STRING, quoted_premium DECIMAL(10,2), quoted_date DATE, expiry_date DATE, converted BOOLEAN, agent_id BIGINT, channel STRING",
            "id + {offset} AS quote_id, floor(rand()*5000000)+1 AS customer_id, element_at(array('auto','home','life','health','business'),cast(floor(rand()*5)+1 as INT)) AS policy_type, round(rand()*5000+100,2) AS quoted_premium, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS quoted_date, date_add('2020-01-01',cast(floor(rand()*1825)+30 as INT)) AS expiry_date, rand() > 0.7 AS converted, floor(rand()*50000)+1 AS agent_id, element_at(array('online','agent','call_center','referral','partner'),cast(floor(rand()*5)+1 as INT)) AS channel"),
        _quick_table("renewals", 200_000, "renewal_id BIGINT, policy_id BIGINT, old_premium DECIMAL(10,2), new_premium DECIMAL(10,2), change_pct DECIMAL(5,2), renewal_date DATE, status STRING, reason STRING",
            "id + {offset} AS renewal_id, floor(rand()*100000000)+1 AS policy_id, round(rand()*5000+100,2) AS old_premium, round(rand()*5000+100,2) AS new_premium, round(rand()*40-10,2) AS change_pct, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS renewal_date, element_at(array('renewed','non_renewed','pending','lapsed'),cast(floor(rand()*4)+1 as INT)) AS status, element_at(array('auto_renew','customer_request','rate_increase','claims_history','competitive'),cast(floor(rand()*5)+1 as INT)) AS reason"),
        _quick_table("compliance_ins", 200_000, "record_id BIGINT, state STRING, regulation STRING, filing_type STRING, filing_date DATE, approval_date DATE, status STRING, department STRING",
            "id + {offset} AS record_id, element_at(array('CA','NY','TX','FL','IL','PA','OH','GA','NC','MI'),cast(floor(rand()*10)+1 as INT)) AS state, element_at(array('rate_filing','form_filing','financial_statement','market_conduct','complaint_response'),cast(floor(rand()*5)+1 as INT)) AS regulation, element_at(array('new','amendment','withdrawal','informational'),cast(floor(rand()*4)+1 as INT)) AS filing_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS filing_date, date_add('2020-01-01',cast(floor(rand()*1825)+60 as INT)) AS approval_date, element_at(array('approved','pending','disapproved','withdrawn'),cast(floor(rand()*4)+1 as INT)) AS status, element_at(array('actuarial','legal','compliance','product','finance'),cast(floor(rand()*5)+1 as INT)) AS department"),
        _quick_table("catastrophe_events", 100_000, "event_id BIGINT, event_name STRING, event_type STRING, start_date DATE, end_date DATE, affected_states STRING, estimated_insured_losses DECIMAL(14,2), claims_count INT, pcs_number STRING",
            "id + {offset} AS event_id, concat(element_at(array('Hurricane','Wildfire','Tornado','Flood','Hailstorm','Winter Storm','Earthquake','Derecho'),cast(floor(rand()*8)+1 as INT)),' ',element_at(array('Alpha','Beta','Gamma','Delta','Epsilon','Zeta','Eta','Theta'),cast(floor(rand()*8)+1 as INT))) AS event_name, element_at(array('hurricane','wildfire','tornado','flood','hail','winter_storm','earthquake','severe_storm'),cast(floor(rand()*8)+1 as INT)) AS event_type, date_add('2020-01-01',cast(floor(rand()*1825) as INT)) AS start_date, date_add('2020-01-01',cast(floor(rand()*1825)+7 as INT)) AS end_date, element_at(array('FL,GA','CA','TX,OK','LA,MS','CO,NE','NY,NJ,CT','CA','IL,IN,OH'),cast(floor(rand()*8)+1 as INT)) AS affected_states, round(rand()*50000000000+100000000,2) AS estimated_insured_losses, cast(floor(rand()*500000)+1000 as INT) AS claims_count, concat('PCS-',lpad(cast(floor(rand()*9999) as STRING),4,'0')) AS pcs_number"),
    ],
    "views": [
        ("v_policy_portfolio", "SELECT policy_type, status, count(*) AS policies, sum(premium) AS total_premium, avg(coverage_limit) AS avg_limit FROM {s}.policies GROUP BY policy_type, status"),
        ("v_claims_summary", "SELECT claim_type, status, count(*) AS claims, sum(amount_claimed) AS total_claimed, sum(amount_paid) AS total_paid FROM {s}.claims_ins GROUP BY claim_type, status"),
        ("v_loss_ratio", "SELECT p.policy_type, sum(l.paid) AS total_losses, sum(p.premium) AS total_premium, round(sum(l.paid)/GREATEST(sum(p.premium),1)*100,2) AS loss_ratio FROM {s}.policies p LEFT JOIN {s}.loss_runs l ON p.policy_id = l.policy_id GROUP BY p.policy_type"),
        ("v_underwriting_results", "SELECT risk_class, decision, count(*) AS applications, avg(risk_score) AS avg_score FROM {s}.underwriting GROUP BY risk_class, decision"),
        ("v_agent_production", "SELECT a.agency, count(DISTINCT a.agent_id) AS agents, sum(a.policies_sold) AS total_sold, sum(a.premium_volume) AS total_premium FROM {s}.agents_ins a WHERE a.status='active' GROUP BY a.agency"),
        ("v_fraud_savings", "SELECT outcome, count(*) AS cases, sum(savings) AS total_savings FROM {s}.fraud_detection GROUP BY outcome"),
        ("v_renewal_retention", "SELECT status, count(*) AS renewals, avg(change_pct) AS avg_premium_change FROM {s}.renewals GROUP BY status"),
        ("v_catastrophe_exposure", "SELECT event_type, count(*) AS events, sum(estimated_insured_losses) AS total_losses, sum(claims_count) AS total_claims FROM {s}.catastrophe_events GROUP BY event_type"),
        ("v_reinsurance_coverage", "SELECT reinsurer, treaty_type, sum(limit_amount) AS total_limit, sum(premium) AS total_premium FROM {s}.reinsurance GROUP BY reinsurer, treaty_type"),
        ("v_quote_conversion", "SELECT policy_type, channel, count(*) AS quotes, sum(CASE WHEN converted THEN 1 ELSE 0 END) AS converted, round(sum(CASE WHEN converted THEN 1 ELSE 0 END)*100.0/count(*),2) AS conversion_pct FROM {s}.quotes GROUP BY policy_type, channel"),
        ("v_payment_collection", "SELECT payment_method, status, count(*) AS payments, sum(amount) AS total FROM {s}.payments_ins GROUP BY payment_method, status"),
        ("v_actuarial_rates", "SELECT table_type, risk_class, age_band, avg(loss_ratio) AS avg_loss_ratio FROM {s}.actuarial_tables GROUP BY table_type, risk_class, age_band"),
        ("v_endorsement_activity", "SELECT endorsement_type, count(*) AS endorsements, avg(premium_change) AS avg_change FROM {s}.endorsements GROUP BY endorsement_type"),
        ("v_compliance_status", "SELECT state, regulation, status, count(*) AS filings FROM {s}.compliance_ins GROUP BY state, regulation, status"),
        ("v_claims_aging", "SELECT status, avg(datediff(current_date(),claim_date)) AS avg_age_days, count(*) AS claims FROM {s}.claims_ins WHERE status NOT IN ('closed','denied') GROUP BY status"),
        ("v_premium_by_state", "SELECT ph.state, p.policy_type, count(*) AS policies, sum(p.premium) AS total_premium FROM {s}.policies p JOIN {s}.policyholders ph ON p.customer_id = ph.customer_id GROUP BY ph.state, p.policy_type"),
        ("v_monthly_written_premium", "SELECT date_trunc('month',effective_date) AS month, policy_type, sum(premium) AS written_premium FROM {s}.policies GROUP BY date_trunc('month',effective_date), policy_type"),
        ("v_combined_ratio", "SELECT p.policy_type, round((sum(l.paid) + sum(p.premium)*0.3)/GREATEST(sum(p.premium),1)*100,2) AS combined_ratio FROM {s}.policies p LEFT JOIN {s}.loss_runs l ON p.policy_id = l.policy_id GROUP BY p.policy_type"),
        ("v_policyholder_demographics", "SELECT gender, floor(datediff(current_date(),date_of_birth)/365) AS age, count(*) AS customers FROM {s}.policyholders GROUP BY gender, floor(datediff(current_date(),date_of_birth)/365)"),
        ("v_fraud_indicators", "SELECT indicators, avg(fraud_score) AS avg_score, count(*) AS detections FROM {s}.fraud_detection WHERE investigation_status IN ('confirmed','investigating') GROUP BY indicators"),
    ],
    "udfs": [
        ("loss_ratio_calc", "losses DECIMAL(14,2), premium DECIMAL(14,2)", "DECIMAL(5,2)", "Loss ratio", "round(losses / GREATEST(premium, 1) * 100, 2)"),
        ("combined_ratio_calc", "loss_ratio DECIMAL(5,2), expense_ratio DECIMAL(5,2)", "DECIMAL(5,2)", "Combined ratio", "round(loss_ratio + expense_ratio, 2)"),
        ("risk_class_label", "risk_class STRING", "STRING", "Risk class display", "initcap(replace(risk_class,'_',' '))"),
        ("premium_rate_change", "old_premium DECIMAL(10,2), new_premium DECIMAL(10,2)", "STRING", "Premium change display", "CASE WHEN new_premium > old_premium THEN concat('+',cast(round((new_premium-old_premium)/old_premium*100,1) as STRING),'%') WHEN new_premium < old_premium THEN concat('-',cast(round((old_premium-new_premium)/old_premium*100,1) as STRING),'%') ELSE 'No Change' END"),
        ("claim_severity_ins", "amount DECIMAL(12,2)", "STRING", "Claim severity", "CASE WHEN amount > 100000 THEN 'Catastrophic' WHEN amount > 50000 THEN 'Major' WHEN amount > 10000 THEN 'Moderate' WHEN amount > 1000 THEN 'Minor' ELSE 'Minimal' END"),
        ("policy_status_label", "status STRING", "STRING", "Policy status", "initcap(replace(status,'_',' '))"),
        ("fraud_risk_ins", "score DOUBLE", "STRING", "Fraud risk level", "CASE WHEN score >= 80 THEN 'Very High' WHEN score >= 60 THEN 'High' WHEN score >= 40 THEN 'Medium' WHEN score >= 20 THEN 'Low' ELSE 'Minimal' END"),
        ("mask_policy_id", "id BIGINT", "STRING", "Masked policy ID", "concat('POL-***',right(cast(id as STRING),4))"),
        ("coverage_adequate", "limit_val DECIMAL(14,2), recommended DECIMAL(14,2)", "STRING", "Coverage adequacy", "CASE WHEN limit_val >= recommended THEN 'Adequate' WHEN limit_val >= recommended*0.75 THEN 'Borderline' ELSE 'Underinsured' END"),
        ("renewal_eligibility", "claims_count INT, premium_paid BOOLEAN", "BOOLEAN", "Eligible for renewal", "claims_count <= 3 AND premium_paid"),
        ("deductible_impact", "claim DECIMAL(12,2), deductible DECIMAL(10,2)", "DECIMAL(12,2)", "Net claim after deductible", "GREATEST(claim - deductible, 0)"),
        ("annualized_premium", "premium DECIMAL(10,2), months INT", "DECIMAL(10,2)", "Annualized premium", "round(premium * 12.0 / GREATEST(months, 1), 2)"),
        ("catastrophe_category", "losses DECIMAL(14,2)", "STRING", "Catastrophe category", "CASE WHEN losses >= 25000000000 THEN 'Mega-Cat' WHEN losses >= 5000000000 THEN 'Major-Cat' WHEN losses >= 1000000000 THEN 'Significant' ELSE 'Minor' END"),
        ("treaty_retention_pct", "retention DECIMAL(14,2), limit_val DECIMAL(14,2)", "DECIMAL(5,2)", "Retention percentage", "round(retention / GREATEST(limit_val, 1) * 100, 2)"),
        ("quote_validity", "expiry_date DATE", "STRING", "Quote validity status", "CASE WHEN expiry_date >= current_date() THEN 'Valid' WHEN datediff(current_date(),expiry_date) <= 7 THEN 'Recently Expired' ELSE 'Expired' END"),
        ("filing_priority", "regulation STRING", "INT", "Filing priority", "CASE WHEN regulation = 'rate_filing' THEN 1 WHEN regulation = 'form_filing' THEN 2 WHEN regulation = 'financial_statement' THEN 3 ELSE 4 END"),
        ("is_high_risk_policy", "risk_score DOUBLE, claims_count INT", "BOOLEAN", "High risk check", "risk_score > 70 OR claims_count > 3"),
        ("age_band", "dob DATE", "STRING", "Age band for actuarial", "CASE WHEN datediff(current_date(),dob)/365 < 25 THEN '18-24' WHEN datediff(current_date(),dob)/365 < 35 THEN '25-34' WHEN datediff(current_date(),dob)/365 < 45 THEN '35-44' WHEN datediff(current_date(),dob)/365 < 55 THEN '45-54' WHEN datediff(current_date(),dob)/365 < 65 THEN '55-64' ELSE '65+' END"),
        ("commission_amount", "premium DECIMAL(10,2), rate DECIMAL(5,2)", "DECIMAL(10,2)", "Agent commission", "round(premium * rate / 100, 2)"),
        ("anonymize_policyholder", "first_name STRING, customer_id BIGINT", "STRING", "Anonymized policyholder", "concat('PH-',substring(sha2(concat(first_name,cast(customer_id as STRING)),256),1,8))"),
    ],
}

# Partition column mapping for large fact tables
PARTITION_COLS = {
    "claims": "submitted_date", "encounters": "encounter_date", "prescriptions": "fill_date",
    "transactions": "txn_date", "card_events": "event_timestamp", "loan_payments": "payment_date",
    "order_items": "shipped_date", "clickstream": "event_timestamp", "reviews": "review_date",
    "cdr_records": "start_time", "data_usage": "session_start", "billing": "billing_period",
    "sensor_readings": "reading_timestamp", "production_events": "event_timestamp", "quality_checks": "check_timestamp",
    "meter_readings": "reading_timestamp", "grid_events": "event_timestamp", "generation_output": "generation_date",
    "enrollments": "enrollment_date", "learning_events": "event_timestamp", "assessments": "submitted_at",
    "listings": "list_date", "transactions_re": "closing_date", "property_views": "view_date",
    "shipments": "ship_date", "tracking_events": "event_timestamp", "fleet_telemetry": "timestamp_utc",
    "policies": "effective_date", "claims_ins": "claim_date", "underwriting": "decision_date",
}

# Foreign key relationships: (child_table, fk_column, parent_table, pk_column)
FK_RELATIONSHIPS = {
    "healthcare": [
        ("claims", "patient_id", "patients", "patient_id"),
        ("claims", "provider_id", "providers", "provider_id"),
        ("encounters", "patient_id", "patients", "patient_id"),
        ("encounters", "provider_id", "providers", "provider_id"),
        ("prescriptions", "patient_id", "patients", "patient_id"),
        ("lab_results", "patient_id", "patients", "patient_id"),
    ],
    "financial": [
        ("transactions", "account_id", "accounts", "account_id"),
        ("loans", "customer_id", "customers", "customer_id"),
        ("cards", "account_id", "accounts", "account_id"),
        ("fraud_alerts", "account_id", "accounts", "account_id"),
    ],
    "retail": [
        ("orders", "customer_id", "customers", "customer_id"),
        ("order_items", "product_id", "products", "product_id"),
        ("reviews", "product_id", "products", "product_id"),
        ("reviews", "customer_id", "customers", "customer_id"),
    ],
    "telecom": [
        ("cdr_records", "subscriber_id", "subscribers", "subscriber_id"),
        ("data_usage", "subscriber_id", "subscribers", "subscriber_id"),
        ("billing", "subscriber_id", "subscribers", "subscriber_id"),
        ("devices", "subscriber_id", "subscribers", "subscriber_id"),
    ],
    "manufacturing": [
        ("production_events", "line_id", "production_lines", "line_id"),
        ("downtime_events", "equipment_id", "equipment", "equipment_id"),
        ("sensor_readings", "equipment_id", "equipment", "equipment_id"),
    ],
    "energy": [
        ("billing_energy", "account_id", "customers_energy", "customer_id"),
        ("generation_output", "plant_id", "power_plants", "plant_id"),
        ("emissions", "plant_id", "power_plants", "plant_id"),
    ],
    "education": [
        ("enrollments", "student_id", "students", "student_id"),
        ("enrollments", "course_id", "courses", "course_id"),
        ("assessments", "student_id", "students", "student_id"),
        ("financial_aid", "student_id", "students", "student_id"),
    ],
    "real_estate": [
        ("listings", "property_id", "properties", "property_id"),
        ("transactions_re", "listing_id", "listings", "listing_id"),
        ("mortgages", "property_id", "properties", "property_id"),
    ],
    "logistics": [
        ("tracking_events", "shipment_id", "shipments", "shipment_id"),
        ("customs_entries", "shipment_id", "shipments", "shipment_id"),
        ("route_plans", "vehicle_id", "vehicles", "vehicle_id"),
        ("route_plans", "driver_id", "drivers", "driver_id"),
    ],
    "insurance": [
        ("claims_ins", "policy_id", "policies", "policy_id"),
        ("underwriting", "policy_id", "policies", "policy_id"),
        ("payments_ins", "policy_id", "policies", "policy_id"),
        ("endorsements", "policy_id", "policies", "policy_id"),
    ],
}

ALL_INDUSTRIES = list(INDUSTRIES.keys())


# ---------------------------------------------------------------------------
# Generator logic
# ---------------------------------------------------------------------------

def generate_demo_catalog(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog_name: str,
    industries: list[str] | None = None,
    owner: str | None = None,
    scale_factor: float = 1.0,
    batch_size: int = 5_000_000,
    max_workers: int = 4,
    storage_location: str | None = None,
    drop_existing: bool = False,
    medallion: bool = True,
    uc_best_practices: bool = True,
    create_functions: bool = True,
    create_volumes: bool = True,
    start_date: str = "2020-01-01",
    end_date: str = "2025-01-01",
    progress_dict: dict | None = None,
) -> dict:
    """Generate a demo catalog with realistic synthetic data.

    Args:
        client: Databricks WorkspaceClient.
        warehouse_id: SQL warehouse ID.
        catalog_name: Name of the catalog to create.
        industries: List of industries to generate. None = all 5.
        owner: Optional catalog owner.
        scale_factor: Row multiplier. 1.0 = ~1B rows total.
        batch_size: Rows per INSERT batch.
        max_workers: Parallel SQL workers.
        storage_location: Optional managed location for catalog.
        drop_existing: Drop existing catalog first.

    Returns:
        Summary dict with counts and timing.
    """
    # Parameter validation
    if not catalog_name or not catalog_name.strip():
        raise ValueError("catalog_name cannot be empty")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', catalog_name):
        raise ValueError(f"Invalid catalog_name '{catalog_name}' — must be alphanumeric with underscores")
    if scale_factor <= 0 or scale_factor > 10:
        raise ValueError(f"scale_factor must be between 0 (exclusive) and 10, got {scale_factor}")
    if batch_size < 1000 or batch_size > 50_000_000:
        raise ValueError(f"batch_size must be between 1000 and 50000000, got {batch_size}")
    if max_workers < 1 or max_workers > 16:
        raise ValueError(f"max_workers must be between 1 and 16, got {max_workers}")
    # Validate dates
    try:
        from datetime import datetime as _dt
        sd = _dt.strptime(start_date, "%Y-%m-%d")
        ed = _dt.strptime(end_date, "%Y-%m-%d")
        if sd >= ed:
            raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")
    except ValueError as ve:
        if "must be before" in str(ve):
            raise
        raise ValueError(f"Invalid date format — use YYYY-MM-DD. start_date={start_date}, end_date={end_date}")

    if industries is None:
        industries = ALL_INDUSTRIES
    else:
        invalid = [i for i in industries if i not in INDUSTRIES]
        if invalid:
            raise ValueError(f"Unknown industries: {invalid}. Valid: {ALL_INDUSTRIES}")
        industries = [i for i in industries if i in INDUSTRIES]
    if not industries:
        raise ValueError(f"No valid industries. Choose from: {ALL_INDUSTRIES}")

    start = time.time()
    result = {
        "catalog": catalog_name,
        "industries": industries,
        "scale_factor": scale_factor,
        "schemas_created": 0,
        "tables_created": 0,
        "views_created": 0,
        "udfs_created": 0,
        "total_rows": 0,
        "errors": [],
    }

    # Step 1: Create catalog
    logger.info(f"Creating catalog: {catalog_name}")
    try:
        if drop_existing:
            try:
                execute_sql(client, warehouse_id, f"DROP CATALOG IF EXISTS `{catalog_name}` CASCADE")
                logger.info(f"Dropped existing catalog {catalog_name}")
            except Exception as e:
                logger.warning(f"Could not drop catalog: {e}")

        loc_clause = f" MANAGED LOCATION '{storage_location}'" if storage_location else ""
        execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`{loc_clause}")

        if owner:
            try:
                execute_sql(client, warehouse_id, f"ALTER CATALOG `{catalog_name}` SET OWNER TO `{owner}`")
                logger.info(f"Set catalog owner to {owner}")
            except Exception as e:
                logger.warning(f"Could not set owner: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to create catalog {catalog_name}: {e}")

    # Step 2: Generate each industry
    for idx, industry in enumerate(industries):
        logger.info(f"Generating industry {idx+1}/{len(industries)}: {industry}")
        if progress_dict is not None:
            progress_dict.update({
                "current_industry": industry,
                "industry_index": idx + 1,
                "total_industries": len(industries),
                "current_phase": "tables",
                "tables_done": 0,
                "tables_total": len(INDUSTRIES[industry].get("tables", [])),
            })
        try:
            counts = _generate_industry(
                client, warehouse_id, catalog_name, industry,
                INDUSTRIES[industry], scale_factor, batch_size, max_workers,
                progress_dict=progress_dict,
                create_functions=create_functions,
                start_date=start_date,
            )
            result["schemas_created"] += 1
            result["tables_created"] += counts["tables"]
            result["views_created"] += counts["views"]
            result["udfs_created"] += counts["udfs"]
            result["total_rows"] += counts["rows"]
        except Exception as e:
            logger.error(f"Error generating {industry}: {e}")
            result["errors"].append(f"{industry}: {e}")

    # Step 3: Generate medallion architecture (parallel across industries)
    if medallion:
        logger.info("Generating medallion architecture (parallel)...")
        if progress_dict is not None:
            progress_dict.update({"current_industry": "medallion", "current_phase": "bronze"})

        # Determine schema naming
        if uc_best_practices:
            bronze_name, silver_name, gold_name = "bronze", "silver", "gold"
        else:
            bronze_name = silver_name = gold_name = None  # Set per industry below

        # Phase 1: Create all bronze schemas, then bronze tables in parallel
        logger.info("  Phase 1: Bronze tables (parallel)...")
        bronze_schema_queries = []
        bronze_table_queries = []
        bronze_meta = []  # (industry, table_name, rows)

        for industry in industries:
            industry_def = INDUSTRIES[industry]
            tables = industry_def.get("tables", [])
            top_tables = sorted(tables, key=lambda t: t["rows"], reverse=True)[:5]
            source_schema = f"{catalog_name}.{industry}"
            bn = bronze_name or f"{industry}_bronze"
            bronze_schema = f"{catalog_name}.{bn}"

            bronze_schema_queries.append(
                f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{bn}` COMMENT 'Bronze raw ingestion layer'"
            )

            for tbl in top_tables:
                name = tbl["name"]
                tbl_prefix = f"{industry}_" if uc_best_practices else ""
                bronze_fqn = f"{bronze_schema}.{tbl_prefix}raw_{name}"
                bronze_rows = int(tbl["rows"] * scale_factor * 0.1)
                bronze_table_queries.append(f"""
                    CREATE TABLE IF NOT EXISTS {bronze_fqn}
                    USING DELTA
                    COMMENT 'Bronze raw ingestion of {industry}.{name}'
                    TBLPROPERTIES ('demo.generated_by' = 'clone-xs', 'demo.layer' = 'bronze', 'demo.source_industry' = '{industry}')
                    AS SELECT *, current_timestamp() AS _ingested_at,
                        concat('s3://raw-bucket/{industry}/{name}/', uuid()) AS _source_file,
                        uuid() AS _raw_id
                    FROM {source_schema}.{name}
                    LIMIT {bronze_rows}
                """)
                bronze_meta.append((industry, name, bronze_rows))

        # Create schemas first (sequential — fast DDL)
        for q in bronze_schema_queries:
            try:
                execute_sql(client, warehouse_id, q)
            except Exception as e:
                logger.warning(f"    Bronze schema creation: {e}")

        # Create bronze tables in parallel
        if bronze_table_queries:
            try:
                execute_sql_parallel(client, warehouse_id, bronze_table_queries, max_workers=max_workers)
            except Exception:
                # Fallback: run individually
                for q in bronze_table_queries:
                    try:
                        execute_sql(client, warehouse_id, q)
                    except Exception as e:
                        logger.warning(f"    Bronze table failed: {e}")

        result["schemas_created"] += len(set(q.split("`.`")[1].split("`")[0] for q in bronze_schema_queries))
        result["tables_created"] += len(bronze_meta)
        result["total_rows"] += sum(r for _, _, r in bronze_meta)
        logger.info(f"    Bronze: {len(bronze_meta)} tables created across {len(industries)} industries")

        # Phase 2: Create all silver views in parallel
        if progress_dict is not None:
            progress_dict.update({"current_phase": "silver"})
        logger.info("  Phase 2: Silver views (parallel)...")
        silver_schema_queries = []
        silver_view_queries = []

        for industry in industries:
            industry_def = INDUSTRIES[industry]
            tables = industry_def.get("tables", [])
            top_tables = sorted(tables, key=lambda t: t["rows"], reverse=True)[:5]
            bn = bronze_name or f"{industry}_bronze"
            sn = silver_name or f"{industry}_silver"
            bronze_schema = f"{catalog_name}.{bn}"
            silver_schema = f"{catalog_name}.{sn}"

            silver_schema_queries.append(
                f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{sn}` COMMENT 'Silver cleaned/validated layer'"
            )

            for tbl in top_tables:
                name = tbl["name"]
                tbl_prefix = f"{industry}_" if uc_best_practices else ""
                silver_fqn = f"{silver_schema}.{tbl_prefix}clean_{name}"
                bronze_fqn = f"{bronze_schema}.{tbl_prefix}raw_{name}"
                silver_view_queries.append(f"""
                    CREATE OR REPLACE VIEW {silver_fqn}
                    COMMENT 'Silver cleaned layer of {industry}.{name}'
                    AS SELECT * EXCEPT(_raw_id, _source_file),
                        _ingested_at AS processed_at
                    FROM {bronze_fqn}
                """)

        for q in silver_schema_queries:
            try:
                execute_sql(client, warehouse_id, q)
            except Exception as e:
                logger.warning(f"    Silver schema creation: {e}")

        if silver_view_queries:
            try:
                execute_sql_parallel(client, warehouse_id, silver_view_queries, max_workers=max_workers)
            except Exception:
                for q in silver_view_queries:
                    try:
                        execute_sql(client, warehouse_id, q)
                    except Exception as e:
                        logger.warning(f"    Silver view creation: {e}")

        result["schemas_created"] += len(set(q.split("`.`")[1].split("`")[0] for q in silver_schema_queries))
        result["views_created"] += len(silver_view_queries)
        logger.info(f"    Silver: {len(silver_view_queries)} views created")

        # Phase 3: Create all gold views in parallel
        if progress_dict is not None:
            progress_dict.update({"current_phase": "gold"})
        logger.info("  Phase 3: Gold views (parallel)...")
        gold_schema_queries = []
        gold_view_queries = []

        for industry in industries:
            sn = silver_name or f"{industry}_silver"
            gn = gold_name or f"{industry}_gold"
            silver_schema = f"{catalog_name}.{sn}"
            gold_schema = f"{catalog_name}.{gn}"
            source_schema = f"{catalog_name}.{industry}"

            gold_schema_queries.append(
                f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{gn}` COMMENT 'Gold aggregated business layer'"
            )

            gold_views = _get_gold_views(industry, silver_schema, source_schema)
            for view_name, view_sql in gold_views:
                vname = f"{industry}_{view_name}" if uc_best_practices else view_name
                gold_fqn = f"{gold_schema}.{vname}"
                gold_view_queries.append(
                    f"CREATE OR REPLACE VIEW {gold_fqn} COMMENT 'Gold aggregate — {industry}' AS {view_sql}"
                )

        for q in gold_schema_queries:
            try:
                execute_sql(client, warehouse_id, q)
            except Exception as e:
                logger.warning(f"    Gold schema creation: {e}")

        if gold_view_queries:
            try:
                execute_sql_parallel(client, warehouse_id, gold_view_queries, max_workers=max_workers)
            except Exception:
                for q in gold_view_queries:
                    try:
                        execute_sql(client, warehouse_id, q)
                    except Exception as e:
                        logger.warning(f"    Gold view creation: {e}")

        result["schemas_created"] += len(set(q.split("`.`")[1].split("`")[0] for q in gold_schema_queries))
        result["views_created"] += len(gold_view_queries)
        logger.info(f"    Gold: {len(gold_view_queries)} views created")

    # Step 4: Post-generation enrichment
    logger.info("Running post-generation enrichment...")
    if progress_dict is not None:
        progress_dict.update({"current_industry": "enrichment", "current_phase": "enrichment", "industry_index": len(industries), "total_industries": len(industries)})
    enrichment_queries = []

    for industry in industries:
        tables = INDUSTRIES[industry].get("tables", [])
        fqn_prefix = f"{catalog_name}.{industry}"

        # 4a: Column comments on key columns (only for columns that exist in the table)
        for tbl in tables[:5]:  # Top 5 tables per industry
            # Parse actual column names from DDL
            ddl_cols = {c.strip().split()[0] for c in tbl.get("ddl_cols", "").split(",") if c.strip()}
            col_comments = _get_column_comments(industry, tbl["name"])
            for col_name, comment in col_comments.items():
                if col_name in ddl_cols:
                    enrichment_queries.append(
                        f"ALTER TABLE {fqn_prefix}.{tbl['name']} ALTER COLUMN {col_name} COMMENT '{comment}'"
                    )

        # 4b: Unity Catalog tags
        pii_tables = _get_pii_tables(industry)
        for tbl_name, tag_value in pii_tables.items():
            enrichment_queries.append(
                f"ALTER TABLE {fqn_prefix}.{tbl_name} SET TAGS ('data_classification' = '{tag_value}')"
            )

        # 4c: Primary key constraints (set NOT NULL first, then add PK)
        for tbl in tables[:5]:
            pk_col = tbl["name"].rstrip("s") + "_id"
            if pk_col in tbl["ddl_cols"] or tbl["ddl_cols"].startswith(pk_col):
                enrichment_queries.append(
                    f"ALTER TABLE {fqn_prefix}.{tbl['name']} ALTER COLUMN {pk_col} SET NOT NULL"
                )
                enrichment_queries.append(
                    f"ALTER TABLE {fqn_prefix}.{tbl['name']} ADD CONSTRAINT pk_{tbl['name']} PRIMARY KEY ({pk_col}) NOT ENFORCED"
                )

    # Execute enrichment sequentially per table to avoid DELTA_METADATA_CHANGED conflicts
    # (parallel ALTER COLUMN on the same table causes concurrent metadata updates)
    if enrichment_queries:
        logger.info(f"  Executing {len(enrichment_queries)} enrichment statements (sequential)...")
        for q in enrichment_queries:
            try:
                execute_sql(client, warehouse_id, q)
            except Exception:
                pass  # Best effort — column may not exist or constraint already present

    # 4c2: Foreign key constraints
    logger.info("  Adding foreign key constraints...")
    fk_queries = []
    for industry in industries:
        fqn_prefix = f"{catalog_name}.{industry}"
        for child, fk_col, parent, pk_col in FK_RELATIONSHIPS.get(industry, []):
            fk_queries.append(
                f"ALTER TABLE {fqn_prefix}.{child} ADD CONSTRAINT fk_{child}_{fk_col} "
                f"FOREIGN KEY ({fk_col}) REFERENCES {fqn_prefix}.{parent}({pk_col}) NOT ENFORCED"
            )
    for q in fk_queries:
        try:
            execute_sql(client, warehouse_id, q)
        except Exception:
            pass  # Best effort — FK may already exist or columns mismatch

    # 4d: Data quality issues (intentional nulls, duplicates, outliers)
    logger.info("  Injecting data quality issues...")
    for industry in industries:
        try:
            _inject_data_quality_issues(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry])
        except Exception as e:
            logger.warning(f"  DQ injection for {industry}: {e}")

    # 4e: Delta version history (UPDATEs to create time travel versions)
    logger.info("  Creating Delta version history...")
    for industry in industries:
        try:
            _create_version_history(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry])
        except Exception as e:
            logger.warning(f"  Version history for {industry}: {e}")

    # 4f: Cross-industry views
    logger.info("  Creating cross-industry views...")
    try:
        _create_cross_industry_views(client, warehouse_id, catalog_name, industries)
    except Exception as e:
        logger.warning(f"  Cross-industry views: {e}")

    # 4g: Managed volumes with sample files
    if create_volumes:
        logger.info("  Creating managed volumes...")
        for industry in industries:
            try:
                _create_volumes(client, warehouse_id, catalog_name, industry)
            except Exception as e:
                logger.warning(f"  Volumes for {industry}: {e}")
    else:
        logger.info("  Skipping volume creation (disabled)")

    # 4h: Pre-populate audit logs
    logger.info("  Pre-populating audit logs...")
    try:
        _create_demo_audit_logs(client, warehouse_id, catalog_name, industries)
    except Exception as e:
        logger.warning(f"  Audit logs: {e}")

    # 4i: Row-level security (column masks on PII columns)
    logger.info("  Applying column masks...")
    for industry in industries:
        try:
            _apply_column_masks(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry])
        except Exception as e:
            logger.warning(f"  Column masks for {industry}: {e}")

    # 4j: Z-ORDER on large tables
    logger.info("  Optimizing large tables...")
    for industry in industries:
        try:
            _optimize_tables(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry])
        except Exception as e:
            logger.warning(f"  Optimize for {industry}: {e}")

    # 4k: Row filters on dimension tables
    logger.info("  Applying row filters...")
    for industry in industries:
        try:
            _apply_row_filters(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry])
        except Exception as e:
            logger.warning(f"  Row filters for {industry}: {e}")

    # 4l: SCD2 columns on dimension tables
    logger.info("  Adding SCD2 columns to dimension tables...")
    for industry in industries:
        try:
            _add_scd2_columns(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry])
        except Exception as e:
            logger.warning(f"  SCD2 for {industry}: {e}")

    # 4m: Information schema / data catalog views
    logger.info("  Creating data catalog views...")
    try:
        _create_info_schema_views(client, warehouse_id, catalog_name)
    except Exception as e:
        logger.warning(f"  Data catalog views: {e}")

    # 4n: Seasonal data patterns
    logger.info("  Applying seasonal data patterns...")
    for industry in industries:
        try:
            _apply_seasonal_patterns(client, warehouse_id, catalog_name, industry, INDUSTRIES[industry], scale_factor=scale_factor)
        except Exception as e:
            logger.warning(f"  Seasonal patterns for {industry}: {e}")

    # 4o: Business-meaningful table comments
    logger.info("  Applying business table comments...")
    for industry in industries:
        try:
            _apply_table_comments(client, warehouse_id, catalog_name, industry)
        except Exception as e:
            logger.warning(f"  Table comments for {industry}: {e}")

    # 4p: CHECK constraints
    logger.info("  Adding CHECK constraints...")
    for industry in industries:
        try:
            _add_check_constraints(client, warehouse_id, catalog_name, industry)
        except Exception as e:
            logger.warning(f"  CHECK constraints for {industry}: {e}")

    # 4q: Grants/permissions (best effort)
    logger.info("  Applying demo grants...")
    try:
        _grant_permissions(client, warehouse_id, catalog_name, industries)
    except Exception as e:
        logger.warning(f"  Grants: {e}")

    # 4r: Save pre-built clone template
    logger.info("  Saving clone template...")
    try:
        _save_clone_template(catalog_name, industries)
    except Exception as e:
        logger.warning(f"  Clone template: {e}")

    result["elapsed_seconds"] = round(time.time() - start, 1)
    logger.info(
        f"Demo catalog '{catalog_name}' generated: "
        f"{result['schemas_created']} schemas, {result['tables_created']} tables, "
        f"{result['views_created']} views, {result['udfs_created']} UDFs, "
        f"~{result['total_rows']:,} rows in {result['elapsed_seconds']}s"
    )
    return result


def _generate_industry(
    client, warehouse_id, catalog, industry_name, industry_def,
    scale_factor, batch_size, max_workers, progress_dict=None,
    create_functions=True, start_date: str = "2020-01-01",
):
    """Generate all objects for a single industry schema."""
    schema = f"`{catalog}`.`{industry_name}`"
    fqn_prefix = f"{catalog}.{industry_name}"

    # Create schema
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    logger.info(f"  Schema: {fqn_prefix}")

    counts = {"tables": 0, "views": 0, "udfs": 0, "rows": 0}

    # Create tables + insert data
    for tbl_idx, tbl_def in enumerate(industry_def["tables"]):
        if progress_dict is not None:
            progress_dict.update({"current_phase": "tables", "tables_done": tbl_idx, "tables_total": len(industry_def["tables"])})
        try:
            rows = _create_and_populate_table(
                client, warehouse_id, fqn_prefix, tbl_def,
                scale_factor, batch_size, max_workers, industry_name,
                start_date=start_date,
            )
            counts["tables"] += 1
            counts["rows"] += rows
        except Exception as e:
            logger.error(f"  Failed table {tbl_def['name']}: {e}")

    if progress_dict is not None:
        progress_dict.update({"current_phase": "views", "tables_done": len(industry_def["tables"]), "tables_total": len(industry_def["tables"])})

    # Create views
    for view_name, view_sql in industry_def.get("views", []):
        try:
            fqn = f"{fqn_prefix}.{view_name}"
            sql = view_sql.replace("{s}", fqn_prefix)
            execute_sql(client, warehouse_id, f"CREATE OR REPLACE VIEW {fqn} AS {sql}")
            counts["views"] += 1
        except Exception as e:
            logger.error(f"  Failed view {view_name}: {e}")

    # Create UDFs (if enabled)
    if not create_functions:
        logger.info(f"  Skipping UDF creation (disabled)")
    for udf_def in (industry_def.get("udfs", []) if create_functions else []):
        try:
            name, params, return_type, comment, body = udf_def
            fqn = f"{fqn_prefix}.{name}"
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE FUNCTION {fqn}({params})
                RETURNS {return_type}
                COMMENT '{comment}'
                RETURN {body}
            """)
            counts["udfs"] += 1
        except Exception as e:
            logger.error(f"  Failed UDF {udf_def[0]}: {e}")

    logger.info(
        f"  {industry_name} done: {counts['tables']} tables, "
        f"{counts['views']} views, {counts['udfs']} UDFs, ~{counts['rows']:,} rows"
    )
    return counts


def _create_and_populate_table(
    client, warehouse_id, schema_prefix, tbl_def,
    scale_factor, batch_size, max_workers, industry_name,
    start_date: str = "2020-01-01",
):
    """Create a table and populate it with data in batches."""
    name = tbl_def["name"]
    fqn = f"{schema_prefix}.{name}"
    target_rows = int(tbl_def["rows"] * scale_factor)

    # Create table with partitioning and business metadata
    partition_clause = ""
    part_col = PARTITION_COLS.get(name)
    if part_col and target_rows >= 10_000_000 and part_col in tbl_def["ddl_cols"]:
        partition_clause = f"\n        PARTITIONED BY ({part_col})"

    sla_tier = "gold" if tbl_def["rows"] >= 50_000_000 else ("silver" if tbl_def["rows"] >= 1_000_000 else "bronze")

    logger.info(f"  Creating table {fqn} (target: {target_rows:,} rows{', partitioned by ' + part_col if partition_clause else ''})")
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {fqn} ({tbl_def['ddl_cols']})
        USING DELTA{partition_clause}
        COMMENT 'Demo {industry_name} {name} table — generated by Clone-Xs'
        TBLPROPERTIES (
            'demo.generated_by' = 'clone-xs',
            'demo.industry' = '{industry_name}',
            'owner_team' = '{industry_name}_data_team',
            'refresh_frequency' = 'daily',
            'sla_tier' = '{sla_tier}',
            'data_quality_score' = '95',
            'retention_days' = '365',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

    if target_rows == 0:
        return 0

    # Fix FK ranges for referential integrity and configurable dates
    insert_expr = _fix_fk_ranges(tbl_def["insert_expr"], industry_name, scale_factor, start_date=start_date)
    actual_batch = min(batch_size, target_rows)
    num_batches = max(1, (target_rows + actual_batch - 1) // actual_batch)

    queries = []
    for i in range(num_batches):
        offset = i * actual_batch
        remaining = target_rows - offset
        this_batch = min(actual_batch, remaining)
        expr = insert_expr.replace("{offset}", str(offset))
        sql = f"INSERT INTO {fqn} SELECT {expr} FROM (SELECT explode(sequence(1, {this_batch})) AS id)"
        queries.append(sql)

    # Execute batches (parallel for large tables, sequential for small)
    total_inserted = 0
    if len(queries) > 1 and max_workers > 1:
        # Run in parallel batches of max_workers
        for chunk_start in range(0, len(queries), max_workers):
            chunk = queries[chunk_start:chunk_start + max_workers]
            try:
                execute_sql_parallel(client, warehouse_id, chunk, max_workers=max_workers)
                total_inserted += sum(
                    min(batch_size, target_rows - (chunk_start + j) * actual_batch)
                    for j in range(len(chunk))
                )
                logger.info(f"    {name}: {total_inserted:,}/{target_rows:,} rows inserted")
            except Exception as e:
                logger.error(f"    Batch failed for {name}: {e}")
                # Try individual queries as fallback
                for q in chunk:
                    try:
                        execute_sql(client, warehouse_id, q)
                        total_inserted += actual_batch
                    except Exception as e2:
                        logger.error(f"    Single insert failed: {e2}")
    else:
        for q in queries:
            try:
                execute_sql(client, warehouse_id, q)
                total_inserted += actual_batch
            except Exception as e:
                logger.error(f"    Insert failed for {name}: {e}")

    logger.info(f"    {name}: {min(total_inserted, target_rows):,} rows complete")
    return min(total_inserted, target_rows)


def _generate_medallion_schemas(
    client, warehouse_id, catalog, industry_name, industry_def,
    scale_factor, batch_size, max_workers, uc_best_practices: bool = True
):
    """Generate bronze/silver/gold medallion schemas for an industry.

    Bronze: Raw ingestion tables with metadata columns (_ingested_at, _source_file, _raw_id)
    Silver: Cleaned/validated tables (deduplicated, nulls handled, typed)
    Gold: Aggregated business-level views

    When uc_best_practices=True (default), schema names follow Unity Catalog conventions:
      bronze, silver, gold (no industry prefix)
    When False, legacy naming: healthcare_bronze, healthcare_silver, healthcare_gold
    """
    counts = {"schemas": 0, "tables": 0, "views": 0, "rows": 0}
    tables = industry_def.get("tables", [])
    if not tables:
        return counts

    # Pick top 5 tables by row count for medallion (don't replicate all 20)
    top_tables = sorted(tables, key=lambda t: t["rows"], reverse=True)[:5]
    source_schema = f"{catalog}.{industry_name}"

    # Schema naming: UC best practices = just "bronze", legacy = "healthcare_bronze"
    if uc_best_practices:
        bronze_name = "bronze"
        silver_name = "silver"
        gold_name = "gold"
    else:
        bronze_name = f"{industry_name}_bronze"
        silver_name = f"{industry_name}_silver"
        gold_name = f"{industry_name}_gold"

    # --- BRONZE ---
    bronze_schema = f"{catalog}.{bronze_name}"
    logger.info(f"  Creating medallion bronze: {bronze_schema}")
    try:
        execute_sql(client, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_name}` "
            f"COMMENT 'Bronze raw ingestion layer'")
        counts["schemas"] += 1

        for tbl in top_tables:
            name = tbl["name"]
            # UC best practice: prefix table with industry to avoid collisions in shared schema
            tbl_prefix = f"{industry_name}_" if uc_best_practices else ""
            bronze_fqn = f"{bronze_schema}.{tbl_prefix}raw_{name}"
            # Bronze = source columns + metadata columns, 10% of original rows
            bronze_rows = int(tbl["rows"] * scale_factor * 0.1)
            try:
                execute_sql(client, warehouse_id, f"""
                    CREATE TABLE IF NOT EXISTS {bronze_fqn}
                    USING DELTA
                    COMMENT 'Bronze raw ingestion of {industry_name}.{name}'
                    TBLPROPERTIES ('demo.generated_by' = 'clone-xs', 'demo.layer' = 'bronze', 'demo.source_industry' = '{industry_name}')
                    AS SELECT *, current_timestamp() AS _ingested_at,
                        concat('s3://raw-bucket/{industry_name}/{name}/', uuid()) AS _source_file,
                        uuid() AS _raw_id
                    FROM {source_schema}.{name}
                    LIMIT {bronze_rows}
                """)
                counts["tables"] += 1
                counts["rows"] += bronze_rows
                logger.info(f"    Bronze: {tbl_prefix}raw_{name} ({bronze_rows:,} rows)")
            except Exception as e:
                logger.error(f"    Failed bronze {tbl_prefix}raw_{name}: {e}")
    except Exception as e:
        logger.error(f"  Failed creating bronze schema: {e}")

    # --- SILVER ---
    silver_schema = f"{catalog}.{silver_name}"
    logger.info(f"  Creating medallion silver: {silver_schema}")
    try:
        execute_sql(client, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{silver_name}` "
            f"COMMENT 'Silver cleaned/validated layer'")
        counts["schemas"] += 1

        for tbl in top_tables:
            name = tbl["name"]
            tbl_prefix = f"{industry_name}_" if uc_best_practices else ""
            silver_fqn = f"{silver_schema}.{tbl_prefix}clean_{name}"
            bronze_fqn = f"{bronze_schema}.{tbl_prefix}raw_{name}"
            # Silver = cleaned views on bronze (no nulls, deduplicated)
            try:
                execute_sql(client, warehouse_id, f"""
                    CREATE OR REPLACE VIEW {silver_fqn}
                    COMMENT 'Silver cleaned layer of {industry_name}.{name}'
                    AS SELECT * EXCEPT(_raw_id, _source_file),
                        _ingested_at AS processed_at
                    FROM {bronze_fqn}
                """)
                counts["views"] += 1
                logger.info(f"    Silver: {tbl_prefix}clean_{name}")
            except Exception as e:
                logger.error(f"    Failed silver {tbl_prefix}clean_{name}: {e}")
    except Exception as e:
        logger.error(f"  Failed creating silver schema: {e}")

    # --- GOLD ---
    gold_schema = f"{catalog}.{gold_name}"
    logger.info(f"  Creating medallion gold: {gold_schema}")
    try:
        execute_sql(client, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{gold_name}` "
            f"COMMENT 'Gold aggregated business layer'")
        counts["schemas"] += 1

        # Create 3-5 gold aggregate views per industry
        gold_views = _get_gold_views(industry_name, silver_schema, source_schema)
        for view_name, view_sql in gold_views:
            try:
                # Prefix gold views with industry name when using shared schemas
                vname = f"{industry_name}_{view_name}" if uc_best_practices else view_name
                gold_fqn = f"{gold_schema}.{vname}"
                execute_sql(client, warehouse_id,
                    f"CREATE OR REPLACE VIEW {gold_fqn} "
                    f"COMMENT 'Gold aggregate — {industry_name}' AS {view_sql}")
                counts["views"] += 1
                logger.info(f"    Gold: {vname}")
            except Exception as e:
                logger.error(f"    Failed gold {view_name}: {e}")
    except Exception as e:
        logger.error(f"  Failed creating gold schema: {e}")

    logger.info(f"  Medallion done for {industry_name}: {counts['schemas']} schemas, {counts['tables']} tables, {counts['views']} views")
    return counts


def _get_gold_views(industry_name, silver_schema, source_schema):
    """Return gold-layer aggregate view definitions per industry."""
    views = {
        "healthcare": [
            ("daily_claims_summary", f"SELECT date_trunc('day', submitted_date) AS day, status, count(*) AS claims, sum(claim_amount) AS total_amount, avg(claim_amount) AS avg_amount FROM {source_schema}.claims GROUP BY date_trunc('day', submitted_date), status"),
            ("provider_performance", f"SELECT provider_id, count(DISTINCT patient_id) AS patients, count(*) AS encounters, avg(duration_minutes) AS avg_visit_minutes FROM {source_schema}.encounters GROUP BY provider_id"),
            ("top_diagnoses", f"SELECT diagnosis_code, count(*) AS frequency, sum(claim_amount) AS total_cost FROM {source_schema}.claims GROUP BY diagnosis_code ORDER BY frequency DESC"),
            ("patient_risk_cohort", f"SELECT CASE WHEN total > 100000 THEN 'Very High' WHEN total > 50000 THEN 'High' WHEN total > 10000 THEN 'Medium' ELSE 'Low' END AS risk_band, count(*) AS patients FROM (SELECT patient_id, sum(claim_amount) AS total FROM {source_schema}.claims GROUP BY patient_id) GROUP BY CASE WHEN total > 100000 THEN 'Very High' WHEN total > 50000 THEN 'High' WHEN total > 10000 THEN 'Medium' ELSE 'Low' END"),
        ],
        "financial": [
            ("daily_txn_volume", f"SELECT txn_date, txn_type, count(*) AS txns, sum(amount) AS total FROM {source_schema}.transactions GROUP BY txn_date, txn_type"),
            ("fraud_summary", f"SELECT alert_type, resolution, count(*) AS alerts, avg(risk_score) AS avg_score FROM {source_schema}.fraud_alerts GROUP BY alert_type, resolution"),
            ("portfolio_health", f"SELECT loan_type, status, count(*) AS loans, sum(principal) AS total_principal FROM {source_schema}.loans GROUP BY loan_type, status"),
            ("customer_value_segments", f"SELECT CASE WHEN balance > 100000 THEN 'HNW' WHEN balance > 25000 THEN 'Affluent' WHEN balance > 5000 THEN 'Mass Affluent' ELSE 'Mass Market' END AS segment, count(*) AS accounts FROM {source_schema}.accounts GROUP BY CASE WHEN balance > 100000 THEN 'HNW' WHEN balance > 25000 THEN 'Affluent' WHEN balance > 5000 THEN 'Mass Affluent' ELSE 'Mass Market' END"),
        ],
        "retail": [
            ("daily_revenue", f"SELECT order_date, count(*) AS orders, sum(total_amount) AS revenue, avg(total_amount) AS aov FROM {source_schema}.orders GROUP BY order_date"),
            ("category_performance", f"SELECT p.category, count(oi.item_id) AS units, sum(oi.total) AS revenue FROM {source_schema}.order_items oi JOIN {source_schema}.products p ON oi.product_id = p.product_id GROUP BY p.category"),
            ("customer_segments", f"SELECT loyalty_tier, count(*) AS customers FROM {source_schema}.customers GROUP BY loyalty_tier"),
            ("product_ratings", f"SELECT p.category, avg(r.rating) AS avg_rating, count(*) AS reviews FROM {source_schema}.reviews r JOIN {source_schema}.products p ON r.product_id = p.product_id GROUP BY p.category"),
        ],
        "telecom": [
            ("daily_network_kpi", f"SELECT cast(start_time as DATE) AS day, status, count(*) AS calls, avg(duration_seconds) AS avg_duration FROM {source_schema}.cdr_records GROUP BY cast(start_time as DATE), status"),
            ("arpu_by_plan", f"SELECT p.name, avg(b.total_amount) AS arpu FROM {source_schema}.billing b JOIN {source_schema}.subscribers s ON b.subscriber_id = s.subscriber_id JOIN {source_schema}.plans p ON s.plan_id = p.plan_id GROUP BY p.name"),
            ("churn_risk_summary", f"SELECT CASE WHEN churn_probability >= 0.8 THEN 'Critical' WHEN churn_probability >= 0.5 THEN 'High' WHEN churn_probability >= 0.3 THEN 'Medium' ELSE 'Low' END AS risk, count(*) AS subscribers FROM {source_schema}.churn_predictions GROUP BY CASE WHEN churn_probability >= 0.8 THEN 'Critical' WHEN churn_probability >= 0.5 THEN 'High' WHEN churn_probability >= 0.3 THEN 'Medium' ELSE 'Low' END"),
            ("data_consumption_trends", f"SELECT app_category, network_type, sum(bytes_downloaded)/1073741824 AS total_gb FROM {source_schema}.data_usage GROUP BY app_category, network_type"),
        ],
        "manufacturing": [
            ("oee_daily", f"SELECT cast(event_timestamp as DATE) AS day, line_id, sum(quantity) AS produced, count(*) AS events FROM {source_schema}.production_events WHERE event_type = 'complete' GROUP BY cast(event_timestamp as DATE), line_id"),
            ("quality_pass_rate", f"SELECT product_code, count(*) AS checks, sum(CASE WHEN result='pass' THEN 1 ELSE 0 END)*100.0/count(*) AS pass_pct FROM {source_schema}.quality_checks GROUP BY product_code"),
            ("downtime_analysis", f"SELECT reason, category, count(*) AS events, sum(duration_minutes) AS total_minutes, sum(cost_impact) AS total_cost FROM {source_schema}.downtime_events GROUP BY reason, category"),
            ("safety_trends", f"SELECT date_trunc('month',incident_date) AS month, severity, count(*) AS incidents FROM {source_schema}.safety_incidents GROUP BY date_trunc('month',incident_date), severity"),
        ],
    }
    # Add defaults for new industries
    return views.get(industry_name, [
        ("daily_summary", f"SELECT current_date() AS report_date, '{industry_name}' AS industry, 'summary' AS metric_type"),
    ])


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

def _get_column_comments(industry: str, table_name: str) -> dict[str, str]:
    """Return column name -> comment mappings for common columns."""
    common = {
        "patient_id": "Unique patient identifier",
        "customer_id": "Unique customer identifier",
        "subscriber_id": "Unique subscriber identifier",
        "account_id": "Financial account identifier",
        "claim_id": "Unique claim reference number",
        "order_id": "Order reference number",
        "product_id": "Product SKU identifier",
        "first_name": "First name (PII - may require masking)",
        "last_name": "Last name (PII - may require masking)",
        "email": "Email address (PII - contains personal data)",
        "phone": "Phone number (PII - contains personal data)",
        "ssn_hash": "Hashed SSN (PII - sensitive)",
        "date_of_birth": "Date of birth (PII)",
        "address": "Street address (PII)",
        "zip_code": "ZIP/postal code",
        "zip": "ZIP/postal code",
        "status": "Current record status",
        "created_date": "Record creation date",
        "created_at": "Record creation timestamp",
    }
    # Return only columns that exist in this table's definition
    return common


def _get_pii_tables(industry: str) -> dict[str, str]:
    """Return table_name -> classification tag for tables containing PII."""
    base = {
        "healthcare": {"patients": "pii_high", "providers": "pii_medium", "claims": "confidential", "prescriptions": "pii_high"},
        "financial": {"customers": "pii_high", "accounts": "confidential", "transactions": "confidential", "cards": "pii_high"},
        "retail": {"customers": "pii_high", "orders": "confidential", "reviews": "public"},
        "telecom": {"subscribers": "pii_high", "cdr_records": "confidential", "devices": "pii_medium"},
        "manufacturing": {"safety_incidents": "confidential", "shift_schedules": "internal"},
        "energy": {"customers_energy": "pii_high", "billing_energy": "confidential"},
        "education": {"students": "pii_high", "financial_aid": "confidential", "instructors": "pii_medium"},
        "real_estate": {"agents": "pii_medium", "properties": "public", "mortgages": "confidential"},
        "logistics": {"drivers": "pii_medium", "customs_entries": "confidential"},
        "insurance": {"policyholders": "pii_high", "claims_ins": "confidential", "fraud_detection": "confidential"},
    }
    return base.get(industry, {})


def _inject_data_quality_issues(client, warehouse_id, catalog, industry, industry_def):
    """Inject intentional data quality issues for DQ tools to detect."""
    tables = industry_def.get("tables", [])
    fqn_prefix = f"{catalog}.{industry}"
    queries = []

    for tbl in tables[:3]:  # Top 3 tables
        name = tbl["name"]
        fqn = f"{fqn_prefix}.{name}"
        cols = tbl["ddl_cols"]

        # Inject NULLs into string columns (1% of rows)
        if "status STRING" in cols:
            queries.append(f"UPDATE {fqn} SET status = NULL WHERE rand() < 0.01")

        # Inject outliers in numeric columns
        if "amount" in cols.lower() or "price" in cols.lower():
            # Find a numeric column
            for col_part in cols.split(","):
                col_part = col_part.strip()
                if "amount" in col_part.lower() and ("DECIMAL" in col_part or "DOUBLE" in col_part):
                    col_name = col_part.split()[0]
                    queries.append(f"UPDATE {fqn} SET {col_name} = {col_name} * 1000 WHERE rand() < 0.001")
                    break

        # Inject duplicate-looking rows (insert 100 copies of first row)
        queries.append(f"""
            INSERT INTO {fqn}
            SELECT * FROM {fqn} LIMIT 100
        """)

    if queries:
        for q in queries:
            try:
                execute_sql(client, warehouse_id, q)
            except Exception:
                pass  # Best effort


def _create_version_history(client, warehouse_id, catalog, industry, industry_def):
    """Run UPDATEs on tables to create Delta version history for time travel demos."""
    tables = industry_def.get("tables", [])
    fqn_prefix = f"{catalog}.{industry}"

    # Pick top 2 tables for version history
    for tbl in tables[:2]:
        name = tbl["name"]
        fqn = f"{fqn_prefix}.{name}"
        cols = tbl["ddl_cols"]

        if "status STRING" in cols:
            # Version 1: Update some statuses
            try:
                execute_sql(client, warehouse_id,
                    f"UPDATE {fqn} SET status = 'archived' WHERE status = 'pending' AND rand() < 0.1")
                logger.info(f"    Version history: {fqn} v+1 (status update)")
            except Exception:
                pass

            # Version 2: Another update
            try:
                execute_sql(client, warehouse_id,
                    f"UPDATE {fqn} SET status = 'reviewed' WHERE status = 'archived' AND rand() < 0.5")
                logger.info(f"    Version history: {fqn} v+2 (status review)")
            except Exception:
                pass


def _create_cross_industry_views(client, warehouse_id, catalog, industries):
    """Create views that JOIN across industry schemas."""
    schema = f"`{catalog}`.`cross_industry`"
    try:
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception:
        return

    views = []

    # Healthcare + Insurance
    if "healthcare" in industries and "insurance" in industries:
        views.append(("healthcare_insurance_claims",
            f"SELECT h.claim_id AS health_claim_id, h.patient_id, h.claim_amount AS health_amount, "
            f"h.status AS health_status, i.claim_id AS ins_claim_id, i.amount_claimed, i.amount_paid "
            f"FROM {catalog}.healthcare.claims h "
            f"LEFT JOIN {catalog}.insurance.claims_ins i ON h.patient_id = i.policy_id "
            f"LIMIT 100000"))

    # Retail + Logistics
    if "retail" in industries and "logistics" in industries:
        views.append(("retail_logistics_fulfillment",
            f"SELECT o.order_id, o.order_date, o.total_amount, "
            f"s.shipment_id, s.carrier, s.status AS ship_status, s.delivery_date "
            f"FROM {catalog}.retail.orders o "
            f"LEFT JOIN {catalog}.logistics.shipments s ON o.order_id = s.order_id "
            f"LIMIT 100000"))

    # Financial + Insurance
    if "financial" in industries and "insurance" in industries:
        views.append(("financial_insurance_risk",
            f"SELECT c.customer_id, c.credit_score, c.risk_rating, "
            f"p.policy_type, p.premium, u.risk_score AS underwriting_score "
            f"FROM {catalog}.financial.customers c "
            f"LEFT JOIN {catalog}.insurance.policies p ON c.customer_id = p.customer_id "
            f"LEFT JOIN {catalog}.insurance.underwriting u ON p.policy_id = u.policy_id "
            f"LIMIT 100000"))

    # Energy + Manufacturing
    if "energy" in industries and "manufacturing" in industries:
        views.append(("energy_manufacturing_consumption",
            f"SELECT pl.name AS line_name, pl.plant AS factory, "
            f"ec.meter_type, ec.value AS energy_value, ec.cost AS energy_cost "
            f"FROM {catalog}.manufacturing.production_lines pl "
            f"LEFT JOIN {catalog}.energy.billing_energy ec ON pl.line_id = ec.account_id "
            f"LIMIT 100000"))

    # Telecom + Retail (customer overlap)
    if "telecom" in industries and "retail" in industries:
        views.append(("telecom_retail_customer_360",
            f"SELECT ts.subscriber_id, ts.first_name, ts.last_name, ts.status AS telecom_status, "
            f"rc.loyalty_tier, rc.created_date AS retail_since "
            f"FROM {catalog}.telecom.subscribers ts "
            f"LEFT JOIN {catalog}.retail.customers rc ON ts.subscriber_id = rc.customer_id "
            f"LIMIT 100000"))

    for view_name, view_sql in views:
        try:
            execute_sql(client, warehouse_id,
                f"CREATE OR REPLACE VIEW {catalog}.cross_industry.{view_name} AS {view_sql}")
            logger.info(f"    Cross-industry view: {view_name}")
        except Exception as e:
            logger.warning(f"    Failed cross-industry view {view_name}: {e}")


def _create_volumes(client, warehouse_id, catalog, industry):
    """Create managed volumes with sample data files."""
    schema = f"`{catalog}`.`{industry}`"
    industry_def = INDUSTRIES.get(industry, {})
    tables = industry_def.get("tables", [])

    for vol_name, comment in [("sample_data", f"Sample data files for {industry}"), ("exports", f"Export destination for {industry} reports")]:
        try:
            execute_sql(client, warehouse_id,
                f"CREATE VOLUME IF NOT EXISTS {schema}.`{vol_name}` COMMENT '{comment}'")
            logger.info(f"    Volume: {catalog}.{industry}.{vol_name}")
        except Exception as e:
            logger.warning(f"    Failed to create volume {vol_name} for {industry}: {e}")

    # Write sample Parquet files to the sample_data volume (top 3 tables, 1000 rows each)
    # Write sample tables (1000 rows each) for the top 3 tables
    for tbl in tables[:3]:
        name = tbl["name"]
        try:
            execute_sql(client, warehouse_id,
                f"CREATE OR REPLACE TABLE {catalog}.{industry}.{name}_sample "
                f"USING DELTA "
                f"COMMENT 'Sample data (1000 rows) from {name}' "
                f"TBLPROPERTIES ('demo.generated_by' = 'clone-xs', 'demo.sample_of' = '{name}') "
                f"AS SELECT * FROM {catalog}.{industry}.{name} LIMIT 1000"
            )
            logger.info(f"    Sample table: {catalog}.{industry}.{name}_sample (1000 rows)")
        except Exception as e:
            logger.warning(f"    Could not create sample for {name}: {e}")


def _create_demo_audit_logs(client, warehouse_id, catalog, industries):
    """Pre-populate audit/run_logs tables with fake clone history so Dashboard shows data."""
    import json
    from datetime import datetime, timedelta
    import random

    # Get audit table config
    try:
        from api.dependencies import get_app_config
        import asyncio
        config = asyncio.get_event_loop().run_until_complete(get_app_config())
    except Exception:
        config = {}

    audit_trail = config.get("audit_trail", {})
    audit_catalog = audit_trail.get("catalog", catalog)
    audit_schema = audit_trail.get("schema", "logs")

    run_logs_fqn = f"{audit_catalog}.{audit_schema}.run_logs"
    ops_fqn = f"{audit_catalog}.{audit_schema}.clone_operations"

    # Generate 20 fake clone operations over the past 30 days
    fake_ops = []
    for i in range(20):
        days_ago = random.randint(0, 30)
        duration = random.randint(30, 600)
        tables_count = random.randint(5, 50)
        success = random.random() > 0.15  # 85% success rate
        src = random.choice(industries)
        dest = f"{src}_clone_{random.randint(1, 5):02d}"

        fake_ops.append({
            "job_id": f"demo-{i:04d}",
            "job_type": random.choice(["clone", "clone", "clone", "sync", "incremental_sync"]),
            "source_catalog": f"{catalog}",
            "destination_catalog": f"{catalog}_{dest}",
            "clone_type": random.choice(["DEEP", "DEEP", "SHALLOW"]),
            "status": "completed" if success else "failed",
            "started_at": f"date_add(current_date(), -{days_ago})",
            "duration_seconds": duration,
            "tables_cloned": tables_count if success else random.randint(0, tables_count),
            "tables_failed": 0 if success else random.randint(1, 5),
            "user_name": random.choice(["admin@company.com", "data-eng@company.com", "platform@company.com"]),
        })

    # Insert into run_logs (if table exists)
    for op in fake_ops:
        sql = f"""
            INSERT INTO {run_logs_fqn}
            (job_id, job_type, source_catalog, destination_catalog, clone_type, status,
             started_at, completed_at, duration_seconds, user_name)
            VALUES (
                '{op["job_id"]}', '{op["job_type"]}', '{op["source_catalog"]}',
                '{op["destination_catalog"]}', '{op["clone_type"]}', '{op["status"]}',
                {op["started_at"]}, date_add({op["started_at"]}, 0),
                {op["duration_seconds"]}, '{op["user_name"]}'
            )
        """
        try:
            execute_sql(client, warehouse_id, sql)
        except Exception as e:
            logger.warning(f"    Audit log insert failed: {e}")
            continue  # Don't stop — try remaining entries

    logger.info(f"    Inserted {len(fake_ops)} demo audit log entries")


def _apply_column_masks(client, warehouse_id, catalog, industry, industry_def):
    """Create column mask functions and apply to PII columns."""
    fqn_prefix = f"{catalog}.{industry}"

    # Define mask functions per industry (create once per catalog)
    mask_functions = {
        "mask_demo_email": ("email STRING", "STRING",
            "concat(left(email, 2), '***@', split(email, '@')[1])"),
        "mask_demo_phone": ("phone STRING", "STRING",
            "concat('***-***-', right(phone, 4))"),
        "mask_demo_name": ("name STRING", "STRING",
            "concat(left(name, 1), repeat('*', length(name) - 1))"),
    }

    for func_name, (params, return_type, body) in mask_functions.items():
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE FUNCTION {fqn_prefix}.{func_name}({params})
                RETURNS {return_type}
                RETURN {body}
            """)
        except Exception:
            pass

    # Apply masks to PII columns (best effort — requires appropriate permissions)
    pii_cols = {
        "healthcare": [("patients", "email", "mask_demo_email"), ("patients", "phone", "mask_demo_phone")],
        "financial": [("customers", "email", "mask_demo_email"), ("customers", "phone", "mask_demo_phone")],
        "retail": [("customers", "email", "mask_demo_email"), ("customers", "phone", "mask_demo_phone")],
        "telecom": [("subscribers", "email", "mask_demo_email"), ("subscribers", "phone", "mask_demo_phone")],
        "education": [("students", "email", "mask_demo_email")],
        "insurance": [("policyholders", "email", "mask_demo_email"), ("policyholders", "phone", "mask_demo_phone")],
        "energy": [("customers_energy", "name", "mask_demo_name")],
        "real_estate": [("agents", "email", "mask_demo_email"), ("agents", "phone", "mask_demo_phone")],
        "logistics": [("drivers", "first_name", "mask_demo_name")],
    }

    for tbl_name, col_name, mask_func in pii_cols.get(industry, []):
        try:
            execute_sql(client, warehouse_id,
                f"ALTER TABLE {fqn_prefix}.{tbl_name} ALTER COLUMN {col_name} "
                f"SET MASK {fqn_prefix}.{mask_func}")
            logger.info(f"    Mask: {fqn_prefix}.{tbl_name}.{col_name} -> {mask_func}")
        except Exception as e:
            logger.warning(f"    Could not set mask on {tbl_name}.{col_name}: {e}")


def _optimize_tables(client, warehouse_id, catalog, industry, industry_def):
    """Run OPTIMIZE and Z-ORDER on large fact tables."""
    tables = industry_def.get("tables", [])
    fqn_prefix = f"{catalog}.{industry}"

    # Only optimize top 3 largest tables
    large_tables = sorted(tables, key=lambda t: t["rows"], reverse=True)[:3]

    for tbl in large_tables:
        name = tbl["name"]
        fqn = f"{fqn_prefix}.{name}"
        cols = tbl["ddl_cols"]

        # Find a good Z-ORDER column (date or status)
        zorder_col = None
        for candidate in ["submitted_date", "txn_date", "event_timestamp", "ship_date",
                          "order_date", "reading_timestamp", "claim_date", "generation_date",
                          "billing_period", "list_date", "enrollment_date", "effective_date"]:
            if candidate in cols:
                zorder_col = candidate
                break

        try:
            if zorder_col:
                execute_sql(client, warehouse_id, f"OPTIMIZE {fqn} ZORDER BY ({zorder_col})")
                logger.info(f"    Optimized: {fqn} ZORDER BY {zorder_col}")
            else:
                execute_sql(client, warehouse_id, f"OPTIMIZE {fqn}")
                logger.info(f"    Optimized: {fqn}")
        except Exception as e:
            logger.warning(f"    Could not optimize {fqn}: {e}")


def _apply_row_filters(client, warehouse_id, catalog, industry, industry_def):
    """Create row filter functions and apply to dimension tables with state/region columns."""
    fqn_prefix = f"{catalog}.{industry}"
    tables = industry_def.get("tables", [])

    # Find dimension tables (rows <= 1M) that have a 'state' or 'country' column
    for tbl in tables:
        if tbl["rows"] > 1_000_000:
            continue
        cols = tbl["ddl_cols"]
        filter_col = None
        if " state STRING" in cols:
            filter_col = "state"
        elif " country STRING" in cols:
            filter_col = "country"
        if not filter_col:
            continue

        func_name = f"row_filter_{tbl['name']}"
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE FUNCTION {fqn_prefix}.{func_name}({filter_col}_val STRING)
                RETURNS BOOLEAN
                COMMENT 'Row filter — restricts access by {filter_col}'
                RETURN is_account_group_member('admins') OR {filter_col}_val IN ('CA','NY','TX')
            """)
            execute_sql(client, warehouse_id,
                f"ALTER TABLE {fqn_prefix}.{tbl['name']} SET ROW FILTER {fqn_prefix}.{func_name} ON ({filter_col})")
            logger.info(f"    Row filter: {fqn_prefix}.{tbl['name']} on {filter_col}")
        except Exception as e:
            logger.warning(f"    Could not set row filter on {tbl['name']}: {e}")
        break  # Only 1 per industry to avoid lock contention


def _add_scd2_columns(client, warehouse_id, catalog, industry, industry_def):
    """Add SCD2 (Slowly Changing Dimension) columns to key dimension tables."""
    fqn_prefix = f"{catalog}.{industry}"
    tables = industry_def.get("tables", [])

    # Pick dimension tables (rows between 100K and 1M)
    dim_tables = [t for t in tables if 100_000 <= t["rows"] <= 1_000_000][:3]

    for tbl in dim_tables:
        fqn = f"{fqn_prefix}.{tbl['name']}"
        tmp_fqn = f"{fqn}_scd2_tmp"
        try:
            # Recreate with SCD2 columns via CTAS (avoids non-deterministic UPDATE issue)
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE TABLE {tmp_fqn}
                USING DELTA
                AS SELECT *,
                    date_add('2020-01-01', cast(floor(rand() * 1825) as INT)) AS valid_from,
                    CASE WHEN rand() > 0.1 THEN DATE '9999-12-31'
                         ELSE date_add('2023-01-01', cast(floor(rand() * 730) as INT)) END AS valid_to,
                    rand() > 0.1 AS is_current
                FROM {fqn}
            """)
            # Safe swap: overwrite original from tmp, then drop tmp
            # This preserves the original table if anything fails
            execute_sql(client, warehouse_id, f"INSERT OVERWRITE TABLE {fqn} SELECT * EXCEPT(valid_from, valid_to, is_current) FROM {tmp_fqn}")
            # Add SCD2 columns to original if not present
            for col, ctype in [("valid_from", "DATE"), ("valid_to", "DATE"), ("is_current", "BOOLEAN")]:
                try:
                    execute_sql(client, warehouse_id, f"ALTER TABLE {fqn} ADD COLUMN {col} {ctype}")
                except Exception:
                    pass
            # Copy SCD2 values from tmp via a second overwrite with all columns
            execute_sql(client, warehouse_id, f"CREATE OR REPLACE TABLE {fqn} AS SELECT * FROM {tmp_fqn}")
            execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {tmp_fqn}")
            logger.info(f"    SCD2: {fqn} (valid_from, valid_to, is_current)")
        except Exception as e:
            # Cleanup tmp if it exists
            try:
                execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {tmp_fqn}")
            except Exception:
                pass
            logger.warning(f"    Could not add SCD2 to {tbl['name']}: {e}")


def _create_info_schema_views(client, warehouse_id, catalog):
    """Create a data_catalog schema with self-documenting views."""
    try:
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`data_catalog`")
    except Exception:
        return

    views = [
        ("table_inventory", f"""
            SELECT table_catalog, table_schema, table_name, table_type, comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'data_catalog')
            ORDER BY table_schema, table_name
        """),
        ("column_inventory", f"""
            SELECT table_schema, table_name, column_name, data_type, is_nullable, comment
            FROM {catalog}.information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'data_catalog')
            ORDER BY table_schema, table_name, ordinal_position
        """),
        ("schema_summary", f"""
            SELECT table_schema,
                count(DISTINCT CASE WHEN table_type = 'TABLE' THEN table_name END) AS tables,
                count(DISTINCT CASE WHEN table_type = 'VIEW' THEN table_name END) AS views
            FROM {catalog}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'data_catalog')
            GROUP BY table_schema
            ORDER BY table_schema
        """),
        ("pii_columns", f"""
            SELECT table_schema, table_name, column_name, data_type, comment
            FROM {catalog}.information_schema.columns
            WHERE (column_name LIKE '%email%' OR column_name LIKE '%phone%'
                OR column_name LIKE '%ssn%' OR column_name LIKE '%name%'
                OR column_name LIKE '%address%' OR column_name LIKE '%dob%'
                OR column_name LIKE '%date_of_birth%')
            AND table_schema NOT IN ('information_schema', 'data_catalog')
            ORDER BY table_schema, table_name
        """),
    ]

    for view_name, view_sql in views:
        try:
            execute_sql(client, warehouse_id,
                f"CREATE OR REPLACE VIEW {catalog}.data_catalog.{view_name} AS {view_sql}")
            logger.info(f"    Data catalog view: {view_name}")
        except Exception as e:
            logger.warning(f"    Could not create data catalog view {view_name}: {e}")


def cleanup_demo_catalog(client, warehouse_id, catalog_name):
    """Remove a demo catalog and all its contents.

    Returns summary of what was cleaned up.
    """
    logger.info(f"Cleaning up demo catalog: {catalog_name}")
    result = {"catalog": catalog_name, "status": "cleaned", "errors": []}

    # Get schema count before dropping
    try:
        schemas = execute_sql(client, warehouse_id,
            f"SELECT schema_name FROM {catalog_name}.information_schema.schemata WHERE schema_name != 'information_schema'")
        result["schemas_dropped"] = len(schemas)
    except Exception:
        result["schemas_dropped"] = 0

    # Get table count
    try:
        tables = execute_sql(client, warehouse_id,
            f"SELECT count(*) AS cnt FROM {catalog_name}.information_schema.tables WHERE table_schema != 'information_schema'")
        result["tables_dropped"] = tables[0]["cnt"] if tables else 0
    except Exception:
        result["tables_dropped"] = 0

    # Drop the catalog
    try:
        execute_sql(client, warehouse_id, f"DROP CATALOG IF EXISTS `{catalog_name}` CASCADE")
        logger.info(f"  Dropped catalog {catalog_name} ({result['schemas_dropped']} schemas, {result['tables_dropped']} objects)")
    except Exception as e:
        result["status"] = "error"
        result["errors"].append(str(e))
        logger.error(f"  Failed to drop catalog: {e}")

    return result


# ---------------------------------------------------------------------------
# Phase 3: Data Quality & Realism helpers
# ---------------------------------------------------------------------------

# FK column → dimension table row count mapping per industry
# Used to ensure FK values fall within valid PK ranges
_FK_DIM_ROWS = {
    "healthcare": {"patient_id": 1_000_000, "provider_id": 1_000_000, "facility_id": 1_000_000, "pharmacy_id": 200_000, "plan_id": 1_000_000, "payer_id": 100},
    "financial": {"account_id": 1_000_000, "customer_id": 1_000_000, "card_id": 1_000_000, "loan_id": 1_000_000, "merchant_id": 500_000, "branch_id": 1_000_000},
    "retail": {"customer_id": 1_000_000, "product_id": 1_000_000, "store_id": 1_000_000, "order_id": 5_000_000, "warehouse_id": 500_000, "supplier_id": 200_000},
    "telecom": {"subscriber_id": 1_000_000, "plan_id": 1_000_000, "tower_id": 1_000_000, "device_id": 1_000_000, "cell_tower_id": 50_000},
    "manufacturing": {"equipment_id": 1_000_000, "line_id": 1_000_000, "material_id": 1_000_000, "supplier_id": 1_000_000, "order_id": 5_000_000, "operator_id": 500},
    "energy": {"meter_id": 500_000, "station_id": 500_000, "plant_id": 1_000_000, "account_id": 500_000, "location_id": 100_000, "customer_id": 500_000},
    "education": {"student_id": 1_000_000, "course_id": 1_000_000, "instructor_id": 500_000, "content_id": 100_000, "grader_id": 5_000, "venue_id": 300},
    "real_estate": {"property_id": 1_000_000, "agent_id": 1_000_000, "listing_id": 100_000_000, "buyer_id": 2_000_000, "seller_id": 2_000_000, "appraiser_id": 10_000, "inspector_id": 5_000},
    "logistics": {"shipment_id": 100_000_000, "vehicle_id": 1_000_000, "driver_id": 1_000_000, "warehouse_id": 1_000_000, "facility_id": 5_000},
    "insurance": {"policy_id": 100_000_000, "customer_id": 1_000_000, "agent_id": 1_000_000, "claim_id": 50_000_000, "adjuster_id": 5_000, "underwriter_id": 500},
}

import re

def _fix_fk_ranges(insert_expr: str, industry_name: str, scale_factor: float, start_date: str = "2020-01-01") -> str:
    """Replace hardcoded FK ranges with scaled ranges for referential integrity.
    Also replaces hardcoded date '2020-01-01' with configurable start_date.
    """
    dim_rows = _FK_DIM_ROWS.get(industry_name, {})
    result = insert_expr

    # Fix FK ranges — use word boundary to avoid partial matches
    for col_name, base_rows in dim_rows.items():
        scaled = max(100, int(base_rows * scale_factor))
        safe_col = re.escape(col_name)
        # Match: floor(rand()*{digits})+1 AS {exact_col_name} (with word boundary)
        pattern = rf"floor\(rand\(\)\*\d+\)\+1 AS {safe_col}\b"
        replacement = f"floor(rand()*{scaled})+1 AS {col_name}"
        result = re.sub(pattern, replacement, result)

    # Replace hardcoded dates with configurable start_date
    if start_date != "2020-01-01":
        result = result.replace("'2020-01-01'", f"'{start_date}'")
        result = result.replace("'2015-01-01'", f"'{start_date}'")  # Some dim tables use older dates

    return result


# Business-meaningful table comments
TABLE_COMMENTS = {
    ("healthcare", "claims"): "Insurance claims submitted by healthcare providers. Contains diagnosis codes (ICD-10), procedure codes (CPT), reimbursement amounts, and adjudication status. Primary fact table for revenue cycle analytics.",
    ("healthcare", "encounters"): "Patient encounters including inpatient, outpatient, emergency, and telehealth visits. Tracks duration, discharge status, and admission source for utilization analysis.",
    ("healthcare", "prescriptions"): "Prescription records including drug codes (NDC), quantities, days supply, and pharmacy information. Key dataset for formulary management and drug utilization review.",
    ("healthcare", "patients"): "Patient demographics including name, date of birth, gender, and insurance information. Contains PII requiring column masking and access controls.",
    ("healthcare", "providers"): "Healthcare provider directory with NPI numbers, specialties, and facility affiliations. Reference table for provider network analysis.",
    ("financial", "transactions"): "Financial transactions across all channels including debit, credit, transfers, and payments. Primary fact table for fraud detection and spending analytics.",
    ("financial", "accounts"): "Customer account master data including checking, savings, money market, and brokerage accounts. Contains balance, interest rate, and overdraft information.",
    ("financial", "customers"): "Customer profiles with credit scores, income brackets, and risk ratings. Contains PII (SSN hash, DOB, email) requiring masking.",
    ("financial", "loans"): "Loan portfolio including mortgage, auto, personal, and student loans. Tracks principal, interest rate, term, and delinquency status.",
    ("retail", "order_items"): "Individual line items within customer orders. Links products to orders with quantity, pricing, discount, and fulfillment status. Largest fact table for sales analytics.",
    ("retail", "customers"): "Customer profiles with loyalty tier, contact information, and registration date. Foundation for customer lifetime value and segmentation analysis.",
    ("retail", "products"): "Product catalog with category, brand, pricing, cost, and active status. Used for margin analysis, assortment planning, and recommendation engines.",
    ("telecom", "cdr_records"): "Call Detail Records capturing every voice call, SMS, and MMS. Includes duration, cell tower, cost, and completion status. Primary dataset for network analytics.",
    ("telecom", "subscribers"): "Subscriber profiles with plan, activation date, credit class, and contract terms. Foundation for churn prediction and customer lifecycle management.",
    ("manufacturing", "sensor_readings"): "Real-time IoT sensor data from production equipment. Captures temperature, pressure, vibration, and other measurements for predictive maintenance.",
    ("manufacturing", "production_events"): "Production floor events including starts, completions, pauses, scrap, and changeovers. Key dataset for OEE (Overall Equipment Effectiveness) analysis.",
    ("energy", "meter_readings"): "Smart meter readings for electric, gas, water, and renewable sources. Foundation for billing, demand forecasting, and conservation programs.",
    ("energy", "generation_output"): "Power generation output by plant and fuel type. Includes capacity factor, CO2 emissions, and dispatch economics for energy portfolio optimization.",
    ("education", "enrollments"): "Student course enrollments with grades, credits, and completion status. Primary dataset for academic analytics, retention analysis, and degree auditing.",
    ("education", "students"): "Student profiles with major, enrollment year, GPA, and financial aid status. Contains PII requiring FERPA compliance controls.",
    ("real_estate", "listings"): "Property listings with asking price, status, property type, and features. Primary dataset for market analysis and comparative market assessments.",
    ("real_estate", "properties"): "Property master data with address, type, year built, lot size, and assessed value. Foundation for appraisal and tax analysis.",
    ("logistics", "shipments"): "Shipment records with origin, destination, carrier, weight, and delivery status. Primary fact table for logistics performance and cost analytics.",
    ("logistics", "tracking_events"): "Package tracking events with location, timestamp, and handler. Provides shipment visibility and SLA compliance monitoring.",
    ("insurance", "policies"): "Insurance policy records across auto, home, life, health, and commercial lines. Contains premium, deductible, coverage limit, and renewal information.",
    ("insurance", "claims_ins"): "Insurance claims with loss details, claimed/paid amounts, and adjuster assignments. Primary dataset for loss ratio analysis and fraud detection.",
}

# CHECK constraint definitions: (table_name, column, expression)
CHECK_CONSTRAINTS = {
    "healthcare": [
        ("claims", "claim_amount", "claim_amount >= 0"),
        ("encounters", "duration_minutes", "duration_minutes >= 0"),
        ("prescriptions", "quantity", "quantity > 0"),
        ("prescriptions", "days_supply", "days_supply > 0"),
    ],
    "financial": [
        ("transactions", "amount", "amount IS NOT NULL"),
        ("loans", "principal", "principal > 0"),
        ("loans", "interest_rate", "interest_rate >= 0 AND interest_rate <= 1"),
        ("accounts", "interest_rate", "interest_rate >= 0"),
    ],
    "retail": [
        ("order_items", "quantity", "quantity > 0"),
        ("order_items", "unit_price", "unit_price >= 0"),
        ("reviews", "rating", "rating BETWEEN 1 AND 5"),
        ("orders", "total_amount", "total_amount >= 0"),
    ],
    "telecom": [
        ("cdr_records", "duration_seconds", "duration_seconds >= 0"),
        ("billing", "total_amount", "total_amount >= 0"),
        ("data_usage", "bytes_downloaded", "bytes_downloaded >= 0"),
    ],
    "manufacturing": [
        ("sensor_readings", "value", "value IS NOT NULL"),
        ("production_events", "quantity", "quantity >= 0"),
        ("downtime_events", "duration_minutes", "duration_minutes >= 0"),
    ],
    "energy": [
        ("generation_output", "output_mwh", "output_mwh >= 0"),
        ("billing_energy", "total", "total >= 0"),
        ("meter_readings", "value", "value >= 0"),
    ],
    "education": [
        ("assessments", "score", "score >= 0 AND score <= 100"),
        ("enrollments", "credits", "credits > 0"),
        ("financial_aid", "amount", "amount > 0"),
    ],
    "real_estate": [
        ("listings", "list_price", "list_price > 0"),
        ("listings", "sqft", "sqft > 0"),
        ("listings", "bedrooms", "bedrooms >= 0"),
    ],
    "logistics": [
        ("shipments", "weight_kg", "weight_kg > 0"),
        ("route_plans", "total_distance_miles", "total_distance_miles > 0"),
    ],
    "insurance": [
        ("policies", "premium", "premium > 0"),
        ("policies", "deductible", "deductible >= 0"),
        ("claims_ins", "amount_claimed", "amount_claimed > 0"),
    ],
}


def _apply_seasonal_patterns(client, warehouse_id, catalog, industry, industry_def, scale_factor=1.0):
    """Shift data dates to create realistic seasonal patterns."""
    fqn_prefix = f"{catalog}.{industry}"
    tables = industry_def.get("tables", [])

    # Industry → peak months and shift logic
    peak_config = {
        "healthcare": (10, 3, "flu/respiratory season"),      # Oct-Mar
        "retail": (10, 12, "holiday shopping season"),         # Oct-Dec
        "energy": (6, 9, "summer cooling peak"),               # Jun-Sep
        "education": (8, 12, "fall semester"),                  # Aug-Dec
        "insurance": (3, 6, "spring severe weather"),           # Mar-Jun
    }

    if industry not in peak_config:
        return  # Financial, telecom, manufacturing, logistics, real_estate — no strong seasonality

    peak_start, peak_end, reason = peak_config[industry]

    # Pick top 2 fact tables with date columns
    for tbl in tables[:2]:
        name = tbl["name"]
        fqn = f"{fqn_prefix}.{name}"
        date_col = PARTITION_COLS.get(name)
        if not date_col:
            continue

        try:
            # Add extra rows with dates shifted into peak period
            # This creates the seasonal spike without non-deterministic UPDATE
            if peak_start <= peak_end:
                month_cond = f"month({date_col}) NOT BETWEEN {peak_start} AND {peak_end}"
                # Shift date into peak: add months to land in peak_start
                shift_expr = f"add_months({date_col}, {peak_start} - month({date_col}))"
            else:
                month_cond = f"month({date_col}) BETWEEN {peak_end + 1} AND {peak_start - 1}"
                shift_expr = f"add_months({date_col}, {peak_start} - month({date_col}) + 12)"

            # Get all columns except the date column, then add shifted date
            all_cols = [c.strip().split()[0] for c in tbl["ddl_cols"].split(",")]
            other_cols = [c for c in all_cols if c != date_col]
            select_cols = ", ".join(other_cols)
            limit_rows = max(1000, int(tbl["rows"] * scale_factor * 0.05))

            execute_sql(client, warehouse_id, f"""
                INSERT INTO {fqn}
                SELECT {select_cols}, {shift_expr} AS {date_col}
                FROM {fqn}
                WHERE {month_cond}
                AND rand() < 0.15
                LIMIT {limit_rows}
            """)
            logger.info(f"    Seasonal: {fqn} — shifted {limit_rows} rows into peak ({reason})")
        except Exception as e:
            logger.warning(f"    Seasonal pattern for {name}: {e}")


def _add_check_constraints(client, warehouse_id, catalog, industry):
    """Add CHECK constraints to validate business rules."""
    fqn_prefix = f"{catalog}.{industry}"
    constraints = CHECK_CONSTRAINTS.get(industry, [])

    for table_name, col_name, expression in constraints:
        try:
            execute_sql(client, warehouse_id,
                f"ALTER TABLE {fqn_prefix}.{table_name} ADD CONSTRAINT "
                f"chk_{table_name}_{col_name} CHECK ({expression}) NOT ENFORCED")
            logger.info(f"    CHECK: {table_name}.{col_name} — {expression}")
        except Exception as e:
            logger.warning(f"    Could not add CHECK on {table_name}.{col_name}: {e}")


def _apply_table_comments(client, warehouse_id, catalog, industry):
    """Apply business-meaningful table comments."""
    for (ind, tbl_name), comment in TABLE_COMMENTS.items():
        if ind != industry:
            continue
        try:
            # Escape single quotes in comment
            safe_comment = comment.replace("'", "\\'")
            execute_sql(client, warehouse_id,
                f"COMMENT ON TABLE {catalog}.{industry}.{tbl_name} IS '{safe_comment}'")
            logger.info(f"    Comment: {industry}.{tbl_name}")
        except Exception as e:
            logger.warning(f"    Could not set comment on {tbl_name}: {e}")


def _grant_permissions(client, warehouse_id, catalog, industries):
    """Grant demo permissions on the catalog (best effort — groups may not exist)."""
    grants = [
        f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `data_analysts`",
        f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `data_engineers`",
        f"GRANT SELECT ON CATALOG `{catalog}` TO `data_analysts`",
        f"GRANT ALL PRIVILEGES ON CATALOG `{catalog}` TO `data_engineers`",
    ]

    for industry in industries:
        grants.append(f"GRANT SELECT ON SCHEMA `{catalog}`.`{industry}` TO `data_analysts`")
        grants.append(f"GRANT ALL PRIVILEGES ON SCHEMA `{catalog}`.`{industry}` TO `data_engineers`")

    for g in grants:
        try:
            execute_sql(client, warehouse_id, g)
        except Exception:
            pass  # Groups may not exist — expected

    logger.info(f"    Grants: applied {len(grants)} permission statements (best effort)")


def _save_clone_template(catalog_name, industries):
    """Save a pre-built clone template config file for the generated catalog."""
    import json
    import os

    template = {
        "name": f"Clone {catalog_name}",
        "description": f"Pre-configured template to clone the demo catalog '{catalog_name}' with all features enabled.",
        "source_catalog": catalog_name,
        "destination_catalog": f"{catalog_name}_clone",
        "clone_type": "DEEP",
        "load_type": "FULL",
        "copy_permissions": True,
        "copy_ownership": True,
        "copy_tags": True,
        "copy_properties": True,
        "copy_security": True,
        "copy_comments": True,
        "copy_constraints": True,
        "validate": True,
        "checksum": False,
        "rollback_on_failure": True,
        "include_schemas": industries,
        "exclude_schemas": [],
        "max_workers": 4,
    }

    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
    os.makedirs(config_dir, exist_ok=True)
    filepath = os.path.join(config_dir, f"demo_clone_{catalog_name}.json")

    try:
        with open(filepath, "w") as f:
            json.dump(template, f, indent=2)
        logger.info(f"    Clone template saved: {filepath}")
    except Exception as e:
        logger.warning(f"    Could not save clone template: {e}")
