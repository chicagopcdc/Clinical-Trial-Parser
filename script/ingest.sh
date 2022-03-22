#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# Ingest clinical studies from the aact db to a csv file. 20 sample studies
# are ingested addressing COVID-19 and non-COVID-19 conditions.
#
# ./script/ingest.sh

set -eu

OUTPUT="data/input/clinical_trials.csv"
DB=aact
LIMIT=10000
PIPE_DELIM_CONDITIONS="acute myeloid leukemia|aml"

QUERY() {
  echo "\COPY (
    with minage_years (nct_id, age) as (
        select nct_id
            , CASE WHEN ctgov.eligibilities.minimum_age like '%Month%' THEN FLOOR(CAST(substring(ctgov.eligibilities.minimum_age, 1, POSITION('Month' in ctgov.eligibilities.minimum_age)-1) as int) / 12.0)
                WHEN ctgov.eligibilities.minimum_age like '%Year%' THEN CAST(substring(ctgov.eligibilities.minimum_age, 1, POSITION('Year' in ctgov.eligibilities.minimum_age)-1) as int)
                WHEN ctgov.eligibilities.minimum_age='N/A' then 0
                END as minage_years 
        from ctgov.eligibilities
        )
        , maxage_years (nct_id, age) as (
        select nct_id
            , CASE WHEN ctgov.eligibilities.maximum_age like '%Month%' THEN FLOOR(CAST(substring(ctgov.eligibilities.maximum_age, 1, POSITION('Month' in ctgov.eligibilities.maximum_age)-1) as int) / 12.0)
                WHEN ctgov.eligibilities.maximum_age like '%Year%' THEN CAST(substring(ctgov.eligibilities.maximum_age, 1, POSITION('Year' in ctgov.eligibilities.maximum_age)-1) as int)
                WHEN ctgov.eligibilities.maximum_age='N/A' then 999
                END as maxage_years 
        from ctgov.eligibilities
        )
        , aggr_conditions (nct_id, conditions) as (
        SELECT nct_id
            , STRING_AGG(name, '|' ORDER BY name) AS conditions 
        FROM conditions 
        GROUP BY nct_id
        )
    SELECT studies.nct_id AS \"#nct_id\"
        , studies.brief_title AS title
        , CASE WHEN calculated_values.has_us_facility THEN 'true' ELSE 'false' END AS has_us_facility
        , aggr_conditions.conditions
        , eligibilities.criteria AS eligibility_criteria
        , minage_years.age as minimum_age
        , maxage_years.age as maximum_age
    FROM studies
        JOIN calculated_values ON studies.nct_id = calculated_values.nct_id
        JOIN aggr_conditions ON studies.nct_id = aggr_conditions.nct_id
        JOIN eligibilities ON studies.nct_id = eligibilities.nct_id
        join minage_years on minage_years.nct_id=studies.nct_id
        join maxage_years on maxage_years.nct_id=studies.nct_id
    WHERE
        LOWER(conditions) ${1} '%($PIPE_DELIM_CONDITIONS)%'
        AND studies.study_type = 'Interventional'
        AND studies.overall_status in ('Recruiting', 'Active, not recruiting')
        and minage_years.age <24
        and calculated_values.has_us_facility
    ORDER BY studies.nct_id DESC      
    LIMIT ${LIMIT}
  )
  TO STDOUT WITH (FORMAT csv, HEADER)
  "
}


psql -U "$USER" -d "$DB" -c "$(QUERY "SIMILAR TO")" > "$OUTPUT"

wc -l "$OUTPUT"
