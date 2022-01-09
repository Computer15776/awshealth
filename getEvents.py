import os
import boto3
import time
from aws_lambda_powertools import Logger

# Configure environment and global vars
ENV = os.environ.get('ENV')
DDB_TABLE = os.environ.get('DDB_TABLE_NAME')


# Configure logging
logger = Logger()

# Setup AWS service clients
health = boto3.client('health')
ddb = boto3.client('dynamodb')

# Paginate DDB events
paginator = health.get_paginator('describe_events')
page_iterator = paginator.paginate(
    filter={
        'eventTypeCategories': ['issue']
    }
)


def create_ddb_kwargs(event):
    '''Generate DDB attributes to populate table with'''

    # ea is shorthand for ExpressionAttributes
    ea_names = {
        '#reg': 'region'
    }
    ea_values = {
        ':statusCode': {
            'S': event.get('statusCode'),
        },
        ':reg': {
            'S': event.get('region'),
        },
        ':service': {
            'S': event.get('service'),
        },
        ':eventDescription': {
            'S': event.get('eventDescription'),
        },
        ':eventTypeCode': {
            'S': event.get('eventTypeCode'),
        },
        ':eventTypeCategory': {
            'S': event.get('eventTypeCategory'),
        },
        ':startTime': {
            'S': str(event.get('startTime')),
        },
        ':lastUpdatedTime': {
            'S': str(event.get('lastUpdatedTime')),
        },
        ':eventScopeCode': {
            'S': event.get('eventScopeCode'),
        },
    }
    update_exp = ("SET"
                  " statusCode= :statusCode,"
                  " #reg= :reg,"
                  " service= :service,"
                  " eventDescription= :eventDescription,"
                  " eventTypeCode= :eventTypeCode,"
                  " eventTypeCategory= :eventTypeCategory,"
                  " startTime= :startTime,"
                  " lastUpdatedTime= :lastUpdatedTime,"
                  " eventScopeCode= :eventScopeCode"
                  )

    if event.get('statusCode') == 'closed':  # Add TTL to remove old status events from DDB
        ea_names = ea_names | {
            '#expires': 'ttl'
        }
        ttl = int(time.time()) + 31556926  # Current time plus 1 POSIX year
        closed_values = {
            ':expires': {
                'N': f'{ttl}',
            },
            ':endTime': {
                'S': f'{event.get("endTime")}',
            },
        }
        ea_values = ea_values | closed_values
        update_exp += ", #expires = if_not_exists(#expires, :expires), endTime= :endTime"

    return ea_names, ea_values, update_exp


def get_event_details(events):
    '''Drill down into each event to obtain event description'''

    event_details = []
    # Max ARNs supported by describe-event-details is 10 per query
    for i in range(0, len(events), 10):
        arn_list = []
        arn_chunk = events[i:i+10]

        for event in arn_chunk:  # Get list of ARNs for use as a param in getting event details
            arn_list.append(event.get('arn'))
        response = health.describe_event_details(eventArns=arn_list)
        valid_events = response.get('successfulSet')

        for event in valid_events:
            try:
                latest_desc = event['eventDescription']['latestDescription']
            except KeyError:
                logger.warn(
                    'latestDescription could not be found for this event.')
            itx = next(
                # loop through and assign event details to each event
                item for item in events if item['arn'] == event['event']['arn'])
            itx['eventDescription'] = latest_desc
            event_details.append(itx)
    return event_details


def update_ddb(full_events):
    '''Update DDB items with retrieved attributes and data'''
    for event in full_events:
        ea_names, ea_values, update_exp = create_ddb_kwargs(event)

        ddb.update_item(
            TableName=DDB_TABLE,
            Key={
                'PK': {
                    'S': 'ARN#' + event['arn']
                }
            },
            UpdateExpression=update_exp,
            ExpressionAttributeNames=ea_names,
            ExpressionAttributeValues=ea_values,
        )


def event_iterator():
    '''Loop through events to obtain data on all available events'''
    event_list = []
    for page in page_iterator:
        for event in page.get('events'):
            # Doubly ensure only public, issue type events are added to the DB
            if ((event.get('eventScopeCode') == 'PUBLIC') and (event.get('eventTypeCategory') == 'issue')):
                event_list.append(event)
    return event_list


@logger.inject_lambda_context
def lambda_handler(event, context):
    try:
        event_list = event_iterator()
        update_ddb(get_event_details(event_list))
    except Exception as e:
        logger.exception(f'An error occurred: {e}')
        raise  # Go no further
