/* These are Pig commands used to preprocess the Medicare database.
   Two preprocessing operations are done.

   First, the 2 county CSV files are combined into one file.  The
   10 fields needed for analysis are extracted.  Also, all entries
   with blank values for contract_id, planid, or segmentid are filtered
   out.

   Second, the services CSV file is filtered so that only English entries
   are stored.  The 5 fields needed for analysis are extracted and any
   entries with blank values for contract_id, planid, or segmentid are not
   used. 

   These commands assume that the 2 county files and services file are
   copied into HDFS in /medicare and renamed to "County1.csv", "County2.csv",
   and "vwPlanServices.csv".

   The output of these commands are 2 CSV files in HDFS in /medicare:
   "County.csv" and "Services.csv".
*/


/* Needed for CSVLoader */
REGISTER /usr/lib/pig/piggybank.jar;

/* Access special CSVLoader that accounts for "," inside of quotes */
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

/* Load County data part 1 */
A = LOAD '/medicare/County1.csv' USING CSVLoader AS (contractid, planid,
      segmentid, year, org, plan, sp_plan, geo, ts, tsd, sp_tsd, type,
      type_desc, web, partd_web, form_web, pharm_web, fas, sp_fas,
      pos, moa, cgo, cgi, cgd, contract_note, sp_contract_note,
      plan_note, sp_plan_note, seg_note, sp_seg_note, legal, trade,
      network, sp_network, contact, address, city, state, zip,
      email_pro, lphone_pro, tphone_pro, ltty_pro, tftty_pro,
      email_cur, lphone_cur, tphone_cur, ltty_cur, tftty_cur,
      contact_pd, address_pd, city_pd, state_pd, zip_pd,
      email_pro_pd, lphone_pro_pd, tphone_pro_pd, ltty_pro_pd, tftty_pro_pd,
      email_cur_pd, lphone_cur_pd, tphone_cur_pd, ltty_cur_pd, tftty_cur_pd,
      mapd, ppopd, snpid, snpdesc, sp_snpdesc, lis100, lis75, lis50, lis25, 
      region, county);

/* Filter out header and records with null ID values */
B = FILTER A BY contractid != 'contract_id' AND contractid !='' AND 
      planid != '' AND segmentid != '';

/* Only save the 10 fields needed for future queries */
C = FOREACH B GENERATE contractid, planid, segmentid, org, plan, 
      address, city, state, zip, county;

/* Load County data part 2 */
A1 = LOAD '/medicare/County2.csv' USING CSVLoader AS (contractid, planid,
       segmentid, year, org, plan, sp_plan, geo, ts, tsd, sp_tsd, type,
      type_desc, web, partd_web, form_web, pharm_web, fas, sp_fas,
      pos, moa, cgo, cgi, cgd, contract_note, sp_contract_note,
      plan_note, sp_plan_note, seg_note, sp_seg_note, legal, trade,
      network, sp_network, contact, address, city, state, zip,
      email_pro, lphone_pro, tphone_pro, ltty_pro, tftty_pro,
      email_cur, lphone_cur, tphone_cur, ltty_cur, tftty_cur,
      contact_pd, address_pd, city_pd, state_pd, zip_pd,
      email_pro_pd, lphone_pro_pd, tphone_pro_pd, ltty_pro_pd, tftty_pro_pd,
      email_cur_pd, lphone_cur_pd, tphone_cur_pd, ltty_cur_pd, tftty_cur_pd,
      mapd, ppopd, snpid, snpdesc, sp_snpdesc, lis100, lis75, lis50, lis25, 
      region, county);

/* Filter out header and records with null ID values */
B1 = FILTER A1 BY contractid != 'contract_id' AND contractid !='' AND 
       planid != '' AND segmentid != '';

/* Only save the 10 fields needed for future queries */
C1 = FOREACH B1 GENERATE contractid, planid, segmentid, org, plan, 
      address, city, state, zip, county;

/* Combine the 2 parts into 1 table */
U  = UNION C, C1;

/* Store the combined table */ 
store U into '/medicare/County.csv' USING PigStorage(',');

/* Load the Services table */
D  = LOAD '/medicare/vwPlanServices.csv' USING CSVLoader AS (language, year,
       contractid, planid, segmentid, category, code, benefit, package_name, 
       package_id, sso);

/* Only take the English records and those with non-null entries for ID */
E  = FILTER D BY language == 'English' AND contractid !='' AND planid != ''
      AND segmentid != '';

/* Save 5 fields for future use */
F  = FOREACH E GENERATE contractid, planid, segmentid, category, benefit;

/* Perform the store */
store F into '/medicare/Services.csv' USING PigStorage(',');


