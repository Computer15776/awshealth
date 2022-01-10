import os
import requests
import textwrap
import time
import json
import boto3
import difflib
from aws_lambda_powertools import Logger
from datetime import datetime, timezone

# Configure logging
logger = Logger()

# Setup AWS service client
ddb = boto3.client('dynamodb')
ssm = boto3.client('ssm')

# Configure environment and global vars
ENV = os.environ.get('ENV')
DDB_TABLE = os.environ.get('DDB_TABLE_NAME')
# Discord API webhook URL
URL = ssm.get_parameter(
    Name=f'/{ENV}/awshealth/URL', WithDecryption=True)['Parameter']['Value']
# URL for failed events to be sent to
FAIL_URL = ssm.get_parameter(
    Name=f'/{ENV}/awshealth/FAIL_URL', WithDecryption=True)['Parameter']['Value']

# The ordering of fieldKeys dictates the field ordering in the Discord embed, they are inline
fieldKeys = ['startTime', 'endTime', 'lastUpdatedTime', 'region']
# Colours to help indicate embed event status
colors = {
    'Issue': 16711680,  # Red - for new issues or reopened
    'Resolved': 65280,  # Green - for existing open issues now closed out
    'Change': 16760576,  # Amber - for ongoing issues that were updated with new info
    'Historical': 38655  # Blue-ish - for closed issues, info is for posterity only
}


@logger.inject_lambda_context
def lambda_handler(event, context):
    '''AWS Lambda event handler'''

    '''details dict contains each attribute as key. 
        'Action' refers to which record types the app will post to Discord 
            when new/updated/removed events occur via DDB.
           i.e. region with 'Action' set to:
                    'PRIVATE' will not display as a delta,
                    'MODIFY' will show the new/old values for region in the embed body
                        (separate to fields which are always present).
        'FullName' is the actual name to use for the attribute when referencing in a string
    '''
    details = {
        'arn': {
            'Action': ['PRIVATE'],
            'FullName': 'Amazon Resource Name'
        },
        'eventDescription': {
            'Action': ['INSERT', 'MODIFY'],
            'FullName': 'Description'
        },
        'region': {
            'Action': ['INSERT', 'MODIFY'],
            'FullName': 'Region',
        },
        'eventScopeCode': {
            'Action': ['PRIVATE'],
            'FullName': 'Event Scope'
        },
        'startTime': {
            'Action': ['PRIVATE'],
            'FullName': 'Start Time'
        },
        'lastUpdatedTime': {
            'Action': ['PRIVATE'],
            'FullName': 'Last Updated'
        },
        'endTime': {
            'Action': ['PRIVATE'],
            'FullName': 'End Time'
        },
        'statusCode': {
            'Action': ['PRIVATE'],
            'FullName': 'Status Code'
        },
        'service': {
            'Action': ['INSERT', 'MODIFY'],
            'FullName': 'Service'
        },
        'eventTypeCategory': {
            'Action': ['PRIVATE'],
            'FullName': 'Event Type Category'
        },
        'eventScopeCode': {
            'Action': ['PRIVATE'],
            'FullName': 'Event Scope'
        },
        'eventTypeCode': {
            'Action': ['PRIVATE'],
            'FullName': 'Event Type'
        },
        'publishedAt': {
            'Action': ['PRIVATE'],
            'FullName': 'Time Published'
        },
        'discordMsgId': {
            'Action': ['PRIVATE'],
            'FullName': 'Discord Message ID'
        },
    }

    try:
        # Only have Lambda parse DDB event stream records; we don't care about Remove events
        if('Records' in event):
            # Should only be 1 event per DDB records batch
            for ev in event.get('Records'):
                logger.debug(f'Event: {ev}')
                action = ev.get('eventName')

                if action != 'REMOVE':
                    # Pull the new/old values into the dict and calc delta for the event description
                    updated_details = load_dict_values(
                        ev, action, details)
                    logger.debug(f'Updated Details: {updated_details}')
                    # Parse data into chunks and then send to Discord webhook
                    handle_event(ev, updated_details)

    except Exception as e:
        logger.exception(f'An error occurred: {e}')
        if FAIL_URL != None:
            requests.post(FAIL_URL, json={'body': context.aws_request_id})
        raise  # We should probably stop right there, no?


def load_dict_values(event, action, details):
    '''Parses the DDB stream event from record and pulls data into the details dict'''

    new_data = event['dynamodb']['NewImage']
    # For every attr with new values in the event that's also in the details dict, append the new value
    # This part is applicable for both INSERT and MODIFY records (INSERT only has 'New')
    for attr in new_data:
        if attr in details:
            details[attr]['NewValue'] = new_data[attr]['S']

    # However if this is a MODIFY record we also want to add the old data, appended to the dict too.
    if action == 'MODIFY':
        old_data = event['dynamodb']['OldImage']
        for key in old_data:
            if key in details:
                details[key]['OldValue'] = old_data[key]['S']
        # If the description has changed, let's run a diff check and generate a delta, then append the delta to the dict
        if old_data.get('eventDescription') != new_data.get('eventDescription'):
            details['eventDescription']['Delta'] = compare_event_descriptions(
                details['eventDescription']['OldValue'], details['eventDescription']['NewValue'])

    # For future use if we need to refer back any metadata from Discord and relate to DDB PK
    details['arn']['NewValue'] = event['dynamodb']['Keys']['PK']['S']
    return details


def compare_event_descriptions(old, new):
    '''Compare new and old values for description and return a delta by analyzing ndiff'''

    old = old.split('\n\n')
    new = new.split('\n\n')
    logger.debug(f'New description is: {old}')
    logger.debug(f'Old description was: {new}')

    diff_list = difflib.ndiff(old, new)
    logger.debug(f'Difflist is: {diff_list}')

    delta_list = []
    for diff in diff_list:
        # If not an unchanged diff line, append to list
        if not diff.startswith('  '):
            delta_list.append(diff)
            logger.debug(f'Diff is: {diff}')

    # Break description delta into chunks as Discord diffs have 1k char limit
    chunk_list = []
    for delta in delta_list:
        # First letter of delta should be diff code i.e. +/-
        diff_code = delta[0]
        delta_chunks = split_message(delta, 990)

        for chunk in delta_chunks:
            # Omit as these are usually junk lines
            if chunk.startswith('?'):
                pass
            # Check that chunk doesn't contain the diff code already
            if not (chunk.startswith('+') or chunk.startswith('-')):
                chunk = f'```diff\n{diff_code} {chunk}\n```'
            elif chunk != ('+ ' or '- '):  # If chunk is not just changed to an empty delta
                chunk = f'```diff\n{chunk}\n```'
            chunk_list.append(chunk)

    logger.debug(f'Delta is: {delta_list}')
    logger.debug(f'Chunk list is: {chunk_list}')
    return chunk_list


def split_message(msg, maxchars):
    '''Splits a string, used to chunk the embeds to abide by Discord char limits'''

    lines = textwrap.wrap(
        msg, maxchars, break_long_words=False, replace_whitespace=False)
    return lines


def handle_event(event, details):
    '''Send data to be constructed into a Discord embed'''

    # Create a list of embeds to be iterated over when pushing to Discord
    embeds = construct_embed(event['eventName'], details)
    logger.debug(f'Embeds: {embeds}')

    # Push embeds to Discord channel
    send_to_discord(embeds)


def construct_embed(action, details):
    '''Constructs the embed out of the constituent parts required'''

    # Create the various components needed for embeds
    embed_title, embed_color = create_embed_header(action, details)
    parameters = create_embed_content(action, details)

    if 'Delta' in details.get('eventDescription'):
        embed_desc = ['**DESCRIPTION CHANGED:**\n'] + \
            details["eventDescription"]["Delta"] + ['\n'] + parameters
    else:
        embed_desc = parameters
    embed_fields = create_embed_fields(details)
    embed_footer = create_embed_footer()

    # Convert desc_chunks into list of lists with 3 chunks in each
    # Desc chunk length is always less than 1k chars per chunk so max of 3 should be fine
    chunk_list = []
    chunk_data = ''
    chunk_chars = 0
    # Loop through all chunks in the embed_desc list and combine into parent chunks of up to 3k chars each
    for i in range(len(embed_desc)):
        if chunk_chars <= 3000:  # Chunk still has space for more characters
            chunk_chars = chunk_chars + len(embed_desc[i])
            chunk_data = chunk_data + ''.join(embed_desc[i])
            i += 1
            logger.debug(f'i: {i}')
            logger.debug(f'Chunk data: {chunk_data}')
            logger.debug(f'Total chunk chars: {chunk_chars}')
        else:  # This chunk is full
            logger.debug('Chunk size is over 3000 chars')
            chunk_list.append(chunk_data)
            chunk_chars = 0
            chunk_data = ''

    chunk_list.append(chunk_data)  # Capture partial chunk and add to embed
    logger.debug(f'Final chunk list: {chunk_list}')

    # Merge child description chunks inside the embed chunks so it's in a human-readable output
    merged_descs = []
    for chunks in chunk_list:
        merged_descs.append(''.join(chunks))

    # Create Discord embeds
    embed_list = []
    for desc in merged_descs:
        embed = {
            'color': embed_color,
            'description': desc
        }
        if (desc == merged_descs[0]):  # First desc chunk i.e header
            embed['title'] = embed_title
        if (desc == merged_descs[-1]):  # Last desc chunk i.e. footer
            embed['fields'] = embed_fields
            embed['footer'] = embed_footer
            embed['timestamp'] = str(datetime.now(timezone.utc))
        embed_list.append(embed)
    return embed_list


def create_embed_header(action, details):
    '''Creates the embed title and determines correct colour'''

    try:
        event_code = details['eventTypeCode']['NewValue'].replace('_', ' ')
        service = details['service']['NewValue']
        new_status = details['statusCode']['NewValue']
    except KeyError as e:
        logger.exception(
            'Error: Could not find essential data for rendering the status event')
    else:
        # Determine the appropriate title and color to use depending on action and statuses
        if action == 'INSERT':
            if new_status == 'open':  # new, open event
                embed_title = f'🚨 **NEW {service} - {event_code} DETECTED** 🚨'
                embed_color = colors.get('Issue')

            elif new_status == 'closed':  # new, closed event
                embed_title = f'📜 **(RESOLVED) {service} - {event_code} DETECTED**'
                embed_color = colors.get('Historical')

        elif action == 'MODIFY':
            old_status = details['statusCode']['OldValue']

            if new_status == 'open' and old_status == 'open':  # no change; still open
                embed_title = f'⚠️ **{service} - {event_code} WAS UPDATED**'
                embed_color = colors.get('Change')

            elif new_status == 'closed' and old_status == 'closed':  # no change; still closed
                embed_title = f'📜 **(RESOLVED) {service} - {event_code}**'
                embed_color = colors.get('Historical')

            elif new_status == 'open' and old_status == 'closed':  # closed -> open
                embed_title = f'🔥 **{service} - {event_code} WAS REOPENED**'
                embed_color = colors.get('Issue')

            elif new_status == 'closed' and old_status == 'open':  # open -> closed
                embed_title = f'🎉 ** {service} - {event_code} WAS RESOLVED**'
                embed_color = colors.get('Resolved')
        return embed_title, embed_color


def create_embed_content(action, details):
    '''Creates the main embed body'''

    parameters = []
    if action == 'INSERT':
        for key in details:
            if 'INSERT' in details[key]['Action']:
                content = f'**{details[key]["FullName"].upper()}:** {details[key]["NewValue"]}\n\n'
                parameters.append(content)

    elif action == 'MODIFY':
        for key in details:
            if 'MODIFY' in details[key]['Action']:
                # Separate out eventDescription as this requires special formatting
                if key != 'eventDescription':
                    if details[key]['NewValue'] != details[key]['OldValue']:
                        content = f'**{details[key]["FullName"].upper()} CHANGED**\n**Updated value:**\n```diff\n+ {details[key]["NewValue"]}\n```**Previous value:**\n```diff\n- {details[key]["OldValue"]}\n```\n'
                        parameters.append(content)

    logger.debug(f'Parameters are: {parameters}')

    return parameters


def create_embed_fields(details):
    '''Creates the ancillary embed footer fields'''

    fieldList = []

    # Create fields by looping through the global var and format accordingly
    for key in fieldKeys:
        try:
            time = int(datetime.fromisoformat(
                details[key]['NewValue']).timestamp())
        except ValueError:  # Field is not able to be coalesced into a datetime object, so create as non-time field
            time = None
        except KeyError:  # Key was removed for whatever reason from the event, so skip this field
            continue

        if time == None:
            field = {
                'name': details[key]['FullName'],
                'value': details[key]['NewValue'],
                'inline': 'True'
            }
        elif key == 'lastUpdatedTime':
            field = {
                'name': details[key]['FullName'],
                'value': f'<t:{time}:R>',
                'inline': 'True'
            }
        else:
            field = {
                'name': details[key]['FullName'],
                'value': f'<t:{time}:F>',
                'inline': 'True'
            }

        fieldList.append(field)
    return fieldList


def create_embed_footer():
    '''Creates the embed footer'''

    footer = {
        "text": "AWS Status Health Monitor",
        "icon_url": "https://a0.awsstatic.com/libra-css/images/logos/aws_logo_smile_1200x630.png"
    }
    return footer


def calculate_discord_delay(r, status):
    '''Determines the correct delay in the event of Discord enforcing a rate limit'''

    # Add a fixed delay so we avoid any potential race condition with rate-limit refresh and message sending
    fixed_delay = float(0.25)
    headers = r.headers
    limit_remaining = headers.get('x-ratelimit-remaining')

    # If the reset-after header is present assign it, otherwise we use fixed, for a marginal delay
    bucket_reset_after = (float(headers.get(
        'x-ratelimit-reset-after')) + fixed_delay) if headers.get('x-ratelimit-reset-after') else fixed_delay
    limit_scope = headers.get('x-ratelimit-scope')

    # Message just sent was fine but we've now hit a limit, so add delay
    if status == 200 and limit_remaining == 0:
        reset_time = bucket_reset_after
        logger.warning('Discord API reqs rate limit now exhausted!')
        logger.warning('Rate limit bucket changes in: ' +
                       reset_time + ' seconds')
    if status == 429:  # Rate limited
        # Get dynamic delay from headers and add slight extra as buffer
        req_retry_after = json.loads(r.text)['retry_after'] + fixed_delay
        # Pick the higher of the two delay values
        reset_time = max(req_retry_after, bucket_reset_after)
        logger.warning('Request was rate limited!')
        logger.warning(f'Rate limit due to {limit_scope} scope')
        logger.warning(f'Message retry available in: {reset_time} seconds')
    else:
        reset_time = fixed_delay  # Still good to go, use slight delay
    return reset_time


def send_to_discord(embeds):
    '''Passes the embed(s) to Discord'''
    if bool(embeds) == False:
        raise TypeError('Embeds is empty. At least one embed must be given.')
    else:
        delay = 0.25
        for embed in embeds:
            status = None
            body = {
                "embeds": [embed]
            }
            while status != 200 and status != None:  # Message not successfully delivered to Discord
                logger.warning(
                    f'Status was {status}. Delaying next request by {delay} seconds')
                time.sleep(delay)

                try:
                    r = requests.post(URL, json=body)
                    status = r.status_code
                    logger.info(f'Discord response status: {status}')
                except:
                    logger.critical(
                        'An error occurred sending the payload to Discord')
                    logger.critical(f'Response: {r.content}')
                    logger.critical(f'Payload:' + {body})
                    logger.critical(f'Delay: ' + {delay})
                else:
                    if status == 200:  # Message success, add metadata back to DDB
                        logger.info(
                            'Payload successfully delivered to Discord')
                        logger.debug(f'Payload:' + {body})
                    else:  # Something happened other than success, log and recalculate delay in case of rate limit
                        logger.critical(f'Status was {status}')
                        logger.critical(f'Embeds: {embeds}')
                        logger.critical(f'Payload: {body}')
                        delay = calculate_discord_delay(r, status)
                    return True
