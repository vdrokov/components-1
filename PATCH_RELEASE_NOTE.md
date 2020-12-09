---
version: 7.1.1
module: https://talend.poolparty.biz/coretaxonomy/42
product:
- https://talend.poolparty.biz/coretaxonomy/23
---

# TPS-4572

| Info             | Value |
| ---------------- | ---------------- |
| Patch Name       | Patch\_20201207\_TPS-4572\_v1-7.1.1 |
| Release Date     | 2020-12-07 |
| Target Version   | 20181026_1147-V7.1.1 |
| Product affected | Talend Studio |

## Introduction

This is a self-contained patch.

**NOTE**: For information on how to obtain this patch, reach out to your Support contact at Talend.

## Fixed issues

This patch contains the following fixes:

- TPS-4572 [7.1.1]tJDBCConnection details are not showing up in the Log even in debug level.(TDI-45146)

## Prerequisites

Consider the following requirements for your system:

- Talend Studio 7.1.1 must be installed.

## Installation

### Installing the patch using Software update

1) Logon TAC and switch to Configuration->Software Update, then enter the correct values and save referring to the documentation: https://help.talend.com/reader/f7Em9WV_cPm2RRywucSN0Q/j9x5iXV~vyxMlUafnDejaQ

2) Switch to Software update page, where the new patch will be listed. The patch can be downloaded from here into the nexus repository.

3) On Studio Side: Logon Studio with remote mode, on the logon page the Update button is displayed: click this button to install the patch.

### Installing the patch using Talend Studio

1) Create a folder named "patches" under your studio installer directory and copy the patch .zip file to this folder.

2) Merge the folder "configuration" and its content to {studio}/configuration and overwrite the existing files.

3) Restart your studio: a window pops up, then click OK to install the patch, or restart the commandline and the patch will be installed automatically.

### Installing the patch using Commandline

Execute the following commands:

1. Talend-Studio-win-x86_64.exe -nosplash -application org.talend.commandline.CommandLine -consoleLog -data commandline-workspace startServer -p 8002 --talendDebug
2. initRemote {tac_url} -ul {TAC login username} -up {TAC login password}
3. checkAndUpdate -tu {TAC login username} -tup {TAC login password}

## Affected files for this patch <!-- if applicable -->

The following files are installed by this patch:

- {Talend\_Studio\_path}/plugins/org.talend.components.jdbc.definition\_0.25.3.jar
- {Talend\_Studio\_path}/configuration/.m2/repository/org/talend/components/components-jdbc-definition/0.25.3/components-jdbc-definition-0.25.3.jar
- {Talend\_Studio\_path}/configuration/.m2/repository/org/talend/components/components-jdbc-runtime/0.25.3/components-jdbc-runtime-0.25.3.jar