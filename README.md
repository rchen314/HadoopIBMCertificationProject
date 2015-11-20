# HadoopIBMCertificationProject
Project to fulfill requirements for IBM Certification in Hadoop.  Analyzes Medicare plan data using Pig and Hive.  Pig is used to scrub the data.  Hive then access these scrubbed databases and performs various queries, such as listing plans by county, organization, whether they offer free ambulance service, whether they offer diabetes and mental health options, etc.

For the results, the first 100 lines were extracted due to some results being
quite large.

This project contains the following files:

   README.md                 This file

   medicare_hdfs_cmds.txt    The Hadoop HDFS commands used for setup.

   medicare_pig_cmds.pig     The Pig commands used to scrub the data

   medicare_hive_cmds.pig    The Hive commands used to perform the queries.

   medicare_report.pdf       Formal report required for certification.  Includes details on the
                             data source, the operations performed on the data, summary of
                             queries run, results, issues encountered and workarounds.

   results                   The results of the Hive queries (see report for more details)

      querybycounty             List plans by county they are offered in.

      querybyorg                List plans by organization offering it.

      querycopay                List plans for a county from most expensive
                                to least expensive copay

      querydiabetes             List diabetes options in plans

      queryfreeambulance        List plans w/ free ambulance support

      querymentalhealth         List mental health options in plans

      querypremium              List plans for a county from lowest premium
                                to highest premium.

