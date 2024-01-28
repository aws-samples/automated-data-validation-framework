# Automated Data Validation Framework User Guide

## Overview:

In migration projects a significant time is spent in doing the data validation and lot of manual efforts being spent. 

The framework developed helps to simplifying this problem by:
- It will help to automate full data validation with some simple config files.
- It will run the framework on EMR and create summary and detail data validation report in S3 and show up on Athena tables.

Only initial effort is to setup this framework and create config files which has table names to compare.

## Visual 1: Architecture.

![visual1](./img/visual1.png)

## Visual 2: Summary Validation Report.

![visual2](./img/visual2.png)

## Visual 3: Detailed Validation Report.

![visual3](./img/visual3.png)

## For Detailed Deployment Instructions and Runbook

<b>Folder Level info</b>
       
1. `data` : It has sample data for each table for your inital understanding.
2. `total_count` : Framework will generate count validation results in this folder.
3. `accuracy` : Framework will generate full data row/columm validation results in this folder.
4. `mismatched_records` : Framework will place the files in this folder, if source and target data has some mismatches.
5. `datavalidation` : This folder has all the initial setup files and jars needed to deploy the solution.
       
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the MIT-0 License.
