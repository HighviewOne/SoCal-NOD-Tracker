{{ config(materialized='view') }}

with source as (
    select * from {{ source('nod_raw', 'nods') }}
),

prep as (
    select *, substr(trim(Situs_Zip), 1, 5) as zip5
    from source
),

cleaned as (
    select
        -- identifiers
        APN                                                     as apn,
        document_number,
        county,

        -- county label ('XX' = out-of-coverage/unknown in the source feed,
        -- typically records with no situs address)
        case
            when coalesce(county, '') != '' then
                case county
                    when 'OC' then 'Orange'
                    when 'RI' then 'Riverside'
                    when 'SD' then 'San Diego'
                    when 'SR' then 'San Bernardino'
                    when 'VE' then 'Ventura'
                    when 'XX' then 'Unknown'
                    else county
                end
            -- Blank code means Los Angeles by feed convention, but ~12% of
            -- blank rows carry zips in neighboring counties. Reassign the
            -- unambiguous cases by zip; zip5 IN-lists cover county enclaves
            -- inside otherwise-LA prefixes (906/907 Orange, 913 Ventura,
            -- 917 San Bernardino/Riverside).
            when zip5 between '91901' and '92199' then 'San Diego'
            when zip5 = '91752' then 'Riverside'  -- Eastvale/Mira Loma
            when zip5 in ('91701','91708','91709','91710','91729','91730',
                          '91737','91739','91743','91758','91759','91761',
                          '91762','91763','91764','91784','91785','91786')
                then 'San Bernardino'  -- Chino, Ontario, Rancho Cucamonga, Upland
            when zip5 between '92201' and '92299' then 'Riverside'
            when zip5 between '92301' and '92499' then 'San Bernardino'
            when zip5 between '92501' and '92599' then 'Riverside'
            when zip5 = '92860' or zip5 between '92877' and '92883'
                then 'Riverside'  -- Norco, Corona, Eastvale
            when zip5 between '92601' and '92899' then 'Orange'
            when zip5 in ('90620','90621','90622','90623','90624','90630',
                          '90680','90720','90721','90740','90742','90743')
                then 'Orange'  -- Buena Park, Cypress, La Palma, Los Alamitos, Seal Beach, Stanton
            when zip5 in ('91319','91320','91358','91360','91361','91362','91377')
                then 'Ventura'  -- Newbury Park, Thousand Oaks, Westlake Village, Oak Park
            when zip5 between '93001' and '93099' then 'Ventura'
            else 'Los Angeles'
        end                                                     as county_name,

        -- property location
        Situs_House                                             as property_house,
        Situs_Street                                            as property_street,
        initcap(trim(Situs_City))                               as property_city,
        Situs_Zip                                               as property_zip,
        zoning,
        use_code,
        safe_cast(yr_built as int64)                            as yr_built,
        safe_cast(bed as int64)                                 as bedrooms,
        safe_cast(bath as float64)                              as bathrooms,
        safe_cast(sq_feet as int64)                             as sq_feet,

        -- owner
        trustor_full_name                                       as owner_name,

        -- financial
        ltv,
        loan_amt,
        min_bid,
        safe_cast(assessed_value as float64)                    as assessed_value,

        -- lender
        beneficiary_name                                        as lender_name,

        -- trustee
        trustee_name,

        -- dates (parse MM/DD/YY or MM/DD/YYYY)
        safe.parse_date('%m/%d/%y',  recording_date)            as recording_date,
        safe.parse_date('%m/%d/%Y',  recording_date)            as recording_date_4y,
        safe.parse_date('%m/%d/%y',  sale_date)                 as sale_date,
        safe.parse_date('%m/%d/%Y',  sale_date)                 as sale_date_4y,
        safe.parse_date('%m/%d/%y',  loan_date)                 as loan_date,

        -- lat/lon
        latitude,
        longtitude                                              as longitude

    from prep
    where APN is not null
        and APN != 'APN'  -- skip header rows if any
)

select
    apn,
    document_number,
    county,
    county_name,
    property_house,
    property_street,
    property_city,
    property_zip,
    zoning,
    use_code,
    yr_built,
    bedrooms,
    bathrooms,
    sq_feet,
    owner_name,
    ltv,
    loan_amt,
    min_bid,
    assessed_value,
    lender_name,
    trustee_name,
    coalesce(recording_date, recording_date_4y)     as recording_date,
    coalesce(sale_date, sale_date_4y)               as sale_date,
    loan_date,
    latitude,
    longitude
from cleaned
