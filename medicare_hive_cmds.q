-- These are a set of Hive commands to do analysis of the Medicare
-- database.  It assumes 2 tables, "County" and "Services", are 
-- created by Pig.   The following analysis is done:
--
--    (1) Show plans grouped by organization.
--    (2) Show plans grouped by county.
--    (3) Show plans with free ambulance service.
--    (4) Show plans with diabetes options.
--    (5) Show plans with mental health care options.
--    (6) Show plans with most expensive copays for a specific county.
--    (7) Show plans with least expensive premiums for a specific county.
--


-- /****************************************/
-- /* Load County table from Pig into Hive */
-- /****************************************/

CREATE TABLE countyTable (
  contractid  STRING,
  planid      STRING,
  segmentid   STRING,
  org         STRING,
  plan        STRING,
  address     STRING,
  city        STRING,
  state       STRING,
  zip         STRING,
  county      STRING)
  COMMENT 'This is medicare county data'
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  LOCATION '/medicare/County.csv';


-- /*************************/
-- /* Query by organization */
-- /*************************/

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/querybyorg'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT org, plan FROM countyTable GROUP BY org,plan;


-- /*******************/
-- /* Query by county */
-- /*******************/

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/querybycounty'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT county, plan, org FROM countyTable 
    GROUP BY county, plan, org;


-- /************************************************************************/
-- /* Load Services table from Pig into Hive -- Needed for next 2 queries. */
-- /************************************************************************/
 
CREATE TABLE servicesTable (
  contractid  STRING,
  planid      STRING,
  segmentid   STRING,
  category    STRING,
  benefit     STRING)
  COMMENT 'This is medicare services data'
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  LOCATION '/medicare/Services.csv';


-- /*************************************************/
-- /* Query for plans with free ambulance services. */
-- /*************************************************/

/* Group by is used to get rid of duplicate entries. */
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/queryfreeambulance'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT plan, org, benefit FROM countyTable JOIN servicesTable 
    ON countyTable.contractID = servicesTable.contractID 
    WHERE benefit = 'If you are admitted to the hospital  you do not have to pay for the ambulance services.'
    GROUP BY plan, org, benefit;


-- /******************************************/
-- /* Query for plans with diabetes options. */
-- /******************************************/

/* Group by is used to get rid of duplicate entries. */
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/querydiabetes'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT plan, org, benefit FROM countyTable JOIN servicesTable 
    ON countyTable.contractID = servicesTable.contractID 
    WHERE category = 'Diabetes Supplies and Services'
    GROUP BY plan, org, benefit;


-- /****************************************************/
-- /* Query for plans with mental health care options. */
-- /****************************************************/

/* Group by is used to get rid of duplicate entries. */
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/querymentalhealth'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT plan, org, benefit FROM countyTable JOIN servicesTable 
    ON countyTable.contractID = servicesTable.contractID 
    WHERE category = 'Mental Health Care'
    GROUP BY plan, org, benefit;


-- /********************/
-- /* Query for copays */
-- /********************/

-- /* First, set up a table with copay information */
CREATE TABLE copays (
  contractid  STRING,
  org         STRING,
  plan        STRING,
  county      STRING,
  benefit     STRING,
  copay       INT)
  COMMENT 'This is a table of copay data created to do sorting by copay'
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  LOCATION '/medicare/Copays.csv';

--/* Second, extract copay information for Doctor's visits and populate the
--   copays table */
INSERT OVERWRITE TABLE copays
  SELECT countyTable.contractid, org, plan, county, benefit,
    regexp_extract(benefit, '([A-Za-z :<>&-]*.&nbsp.<b>.)([0-9]*)', 2) copay
  FROM countyTable JOIN servicesTable
  ON countyTable.contractid = servicesTable.contractid
  WHERE servicesTable.category = "Doctor's Office Visits";

-- /* Finally, do a query grouping by copay and sorting from cheapest to most
--    expensive copay for a specific county. */
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/querycopay'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT county, plan, org, benefit, copay 
    FROM copays
    WHERE county = "39113" 
    GROUP BY county, plan, org, benefit, copay
    ORDER BY copay DESC;


-- /**********************/
-- /* Query for premiums */
-- /**********************/

-- /* First, set up a table with premium information */
CREATE TABLE premiums (
  contractid  STRING,
  org         STRING,
  plan        STRING,
  county      STRING,
  benefit     STRING,
  premium     INT)
  COMMENT 'This is a table of premium data created to do sorting by premium'
  ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  LOCATION '/medicare/Premiums.csv';

-- /* Second, extract premium information for Doctor's visits and populate the
--    premiums table.  The WHERE clauses are used to narrow down which entries
--    in the servicesTable have monthly premium information.  */
INSERT OVERWRITE TABLE premiums
  SELECT countyTable.contractid, org, plan, county, benefit,
    regexp_extract(benefit,'....([0-9]*.[0-9]*).... per month', 1) premium
  FROM countyTable JOIN servicesTable
  ON countyTable.contractid = servicesTable.contractid
  WHERE category = "Monthly Premium  Deductible  and Limits on How Much You Pay for Covered Services" AND instr(benefit, "per month") <> 0 AND substr(benefit, 1, 4) = "<b>$";

-- /* Finally, do a query grouping by premium and sorting from cheapest to most
--    expensive copay for a specific county. */
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/ibm/hive/querypremium'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  SELECT county, plan, org, benefit, premium 
    FROM premiums
    WHERE county = "39113" 
    GROUP BY county, plan, org, benefit, premium
    ORDER BY premium;


