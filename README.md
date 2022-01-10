# AWS Health Discord Status Monitor

![awshealth on Discord](https://i.imgur.com/tkrP2zh.png)

## ℹ️ About
awshealth is a utility that tracks public AWS Status events via the Health API. It stores pertinent event details for one year in a DynamoDB table, and then posts to a Discord webhook when status events occur, via DDB streams. Upon a prior existing event having one or more attributes changed; i.e. the event description is updated, awshealth will calculate the delta of the changes and post only the reflected changes.

This utility is split into two parts; `getEvents.py` which obtains a list of all AWS Health events and stores them in a DDB table, and `publishEvent.py` which publishes to Discord any new or modified events as they are written into the table.

## ⚙️ Configuration
### Environment Variables
For `getEvents.py`:

| Name      | Type | Required | Default  | Description                                                                                                   |
| --------- | ---- | -------- | -------- | ------------------------------------------------------------------------------------------------------------- |
| ENV       | str  | Yes      | -        | The environment the application is running in, i.e. Development/Production                                    |
| DDB_TABLE | str  | Yes      | -        | Table name for DDB to store event data in                                                                     |
| TTL       | int  | No       | 31556926 | The maximum time-to-live in seconds for status events in DynamoDB befoe they are removed. Defaults to 1 year. |

For `publishEvent.py`:

| Name      | Type | Required | Default | Description                                                                |
| --------- | ---- | -------- | ------- | -------------------------------------------------------------------------- |
| ENV       | str  | Yes      | -       | The environment the application is running in, i.e. Development/Production |
| DDB_TABLE | str  | Yes      | -       | Table name for DDB to store event data in                                  |
| URL       | str  | Yes      | -       | The Discord API Webhook URL to post status events to a given channel       |
| FAIL_URL  | str  | No       | -       | Posts to a secondary channel when events encounter errors                  |

## 📜 Requirements
* Python 3.9+
  * os
  * boto3
  * time
  * aws_lambda_powertools
  * requests
  * textwrap
  * json
  * difflib
  * datetime