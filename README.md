# agent-pars-udf
UDF to parse user-agent header from http request

## how to user
- mvn:package
- go to target folder and copy jar to hdfs
- add jar as resource to HUE query executor tab
- define temporary function in setting tab of query tab like **agent_pars** backed by **com.epam.hive.udf.AgentParsUDF**
- use it in you queries like **select agent_pars(user_agent_column) from table_1**
