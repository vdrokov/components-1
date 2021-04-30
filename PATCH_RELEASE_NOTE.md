---
version: 7.3.1
module: https://talend.poolparty.biz/coretaxonomy/42
product: https://talend.poolparty.biz/coretaxonomy/183
---

# TPS-4449

| Info             | Value |
| ---------------- | ---------------- |
| Patch Name       | Patch\_20210430_TPS-4449\_v1-7.3.1 |
| Release Date     | 2021-04-30 |
| Target Version   | 20200219\_1130-7.3.1 |
| Product affected | Talend Data Preparation |

## Introduction

This is a self-contained patch.

**NOTE**: For information on how to obtain this patch, reach out to your Support contact at Talend.

## Fixed issues

This patch contains the following fixes:

- TDI-45852 HDFS dataset headers are not displayed though Set header is enabled

## Prerequisites

Consider the following requirements for your system:

- Talend Data Preparation 7.3.1 must be installed.


## Installation

    1) Shut down Talend Dataprep Service/Component Web Service if it is opened.
    2) Extract the zip.
    3) Merge the folder "service" and its content to {Dataprep}/service and overwrite the existing files.
    4) Start the Dataprep Service/Component Web Service.

## Uninstallation

Backup the Affected files list below. Uninstall the patch by restore the backup files.

## Affected files for this patch

The following files are installed by this patch:

- {Talend\_Dataprep\_path}/services\tcomp\.m2\org\talend\components\simplefileio-runtime\0.27.4\simplefileio-runtime-0.27.4.jar