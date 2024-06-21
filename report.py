import boto3
import openpyxl
import pytz
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta,timezone,date,time
from dateutil.relativedelta import relativedelta
import urllib
import json
import openpyxl
import re
import io
from io import BytesIO
import base64

def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['graph_secret_email_auto','graph_client_email_auto','graph_tenant_id','jira_api_token','email','aws_other_instance_id','aws_secret_key','aws_access_key','aws_arn']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

jira_api_token = extracted_secrets['jira_api_token']['jira_api_token']
graph_secret = extracted_secrets['graph_secret_email_auto']['graph_secret_email_auto']
graph_client_id = extracted_secrets['graph_client_email_auto']['graph_client_email_auto']
graph_tenant_id = extracted_secrets['graph_tenant_id']['graph_tenant_id']
jira_user = extracted_secrets['email']['email']
aws_secret_key = extracted_secrets['aws_secret_key']['aws_secret_key']
aws_instance_id = extracted_secrets['aws_other_instance_id']['aws_other_instance_id']
aws_access_key = extracted_secrets['aws_access_key']['aws_access_key']
aws_arn = extracted_secrets['aws_arn']['aws_arn']

jira_url = "https://hhaxsupport.atlassian.net"
api_endpoint = "/rest/api/3/search/"

jql_query = """project in (HHA,ESD,RCOSD,EAS) and ("Primary Location" ~ NC OR "HHAX Market" ~ NC OR "State" = NC) and (created >= startofday(-30d) and created < startofday(-1d)) ORDER BY created ASC"""

jql_query_encoded = urllib.parse.quote(jql_query)

startAt = 0
maxResults = 100

all_issues = []

while True:
    api_url = f"{jira_url}{api_endpoint}?jql={jql_query_encoded}&startAt={startAt}&maxResults={maxResults}"

    response = requests.get(
        api_url,
        auth=HTTPBasicAuth(jira_user, jira_api_token),
        headers={
            "Accept": "application/json"
        }
    )

    json_response = response.json()

    if response.status_code == 200:
        all_issues.extend(json_response['issues'])

        if json_response['total'] == len(all_issues):
            break
        else:
            startAt += maxResults
    else:
        break

if isinstance(json_response, str):
    json_response = json.loads(json_response)

issues = all_issues

if isinstance(issues, list):
    data = []

    for issue in issues:

        key = issue['key']
        hhax_market = issue['fields'].get('customfield_10206', None)
        primary_location = issue['fields'].get('customfield_12755', None)
        customfield_11540_obj = issue['fields'].get('customfield_11540', {})
        if customfield_11540_obj:
            state = customfield_11540_obj.get('value', None)
        else:
            state = None
        created = issue['fields'].get('created', None)
        payer_obj = issue['fields'].get('customfield_10219', {})
        if payer_obj:
            payer = payer_obj.get('value', None)
        else:
            payer = None
        summary = issue['fields'].get('summary', None)
        resolved = issue['fields'].get('resolutiondate',None)
        status_snapshot = issue['fields'].get('status', {}).get('name', None)
        tax_id = issue['fields'].get('customfield_10204',None)
        updated = issue['fields'].get('updated',None)
        associations_obj = issue['fields'].get('customfield_11478', {})
        if associations_obj and 'content' in associations_obj:
            content_list = associations_obj['content']
            if content_list and 'content' in content_list[0]:
                text_content = content_list[0]['content']
                if text_content and 'text' in text_content[0]:
                    associations = text_content[0]['text']
                else:
                    associations = None
            else:
                associations = None
        else:
            associations = None

        customfield_10236_obj = issue['fields'].get('customfield_10236', {})
        if customfield_10236_obj and 'content' in customfield_10236_obj:
            content_list = customfield_10236_obj['content']
            if content_list and 'content' in content_list[0]:
                text_content = content_list[0]['content']
                if text_content and 'text' in text_content[0]:
                    hhax_regional_platform_tag = text_content[0]['text']
                else:
                    hhax_regional_platform_tag = None
            else:
                hhax_regional_platform_tag = None
        else:
            hhax_regional_platform_tag = None
        data.append([key,hhax_regional_platform_tag,state,primary_location,hhax_market,associations,created,resolved,updated,payer,status_snapshot,summary,tax_id])

    df = pd.DataFrame(data, columns=['key','hhax_platform_region_tag','state','primary_location','hhax_market','associations','create_date','resolved_date','updated','payer','status','summary','tax_id'])
    
df['create_date'] = pd.to_datetime(df['create_date'], utc=True)
df['create_date'] = df['create_date'].dt.tz_convert('US/Eastern')
df['temp_create_date'] = pd.to_datetime(df['create_date']).dt.date
today = datetime.now().date()
yesterday = (today - timedelta(days=1))
cutoff_date = (today - timedelta(days=8))

summary_df = df[(df['temp_create_date'] >= cutoff_date) & (df['temp_create_date'] < yesterday)]
summary_df.drop(columns='temp_create_date', inplace=True)

summary_df['create_date'] = pd.to_datetime(summary_df['create_date']).dt.date
summary_df['resolved_date'] = pd.to_datetime(summary_df['resolved_date'], errors='coerce').dt.date

summary_df['project_prefix'] = summary_df['key'].apply(lambda x: re.match(r'^[A-Z]+', x).group(0) if re.match(r'^[A-Z]+', x) else '')

pivot_data = summary_df.groupby(['create_date', 'project_prefix'])['key'].count().reset_index()

pivot_data = pd.pivot_table(
    pivot_data,
    index='create_date',
    columns='project_prefix',
    values='key',
    aggfunc='sum',
    fill_value=0
)

pivot_data['created'] = summary_df.groupby('create_date')['key'].count()
pivot_data['closed'] = summary_df[summary_df['resolved_date'].notna()].groupby('create_date')['key'].count().reindex(pivot_data.index, fill_value=0)
df.drop(columns='temp_create_date',inplace=True)
df['create_date'] = pd.to_datetime(df['create_date']).dt.date
df['resolved_date'] = pd.to_datetime(df['resolved_date'], errors='coerce').dt.date
df['updated'] = pd.to_datetime(df['updated'], errors='coerce').dt.date

session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1')

connect_client = session.client('connect',endpoint_url='https://connect.us-east-1.amazonaws.com')

queues = connect_client.list_queues(InstanceId=aws_instance_id,QueueTypes=['STANDARD'])

queue_df = pd.DataFrame(queues['QueueSummaryList'])

eastern = pytz.timezone('US/Eastern')
end_date_temp = (yesterday - timedelta(days=1))
start_date = eastern.localize(datetime.combine(cutoff_date, datetime.min.time()))
end_date = eastern.localize(datetime.combine(end_date_temp, datetime.max.time()))

data = []

current_date = start_date

while current_date <= end_date:
    next_date = current_date + timedelta(days=1)
    
    response = connect_client.get_metric_data_v2(
        ResourceArn=f'arn:aws:connect:us-east-1:{aws_arn}:instance/{aws_instance_id}',
        StartTime=current_date,
        EndTime=next_date,
        Filters=[
            {
                'FilterKey': 'QUEUE',
                'FilterValues': ['88e49d6b-269f-46a3-96b8-07734bd6fb53'] 
            },
        ],
        Groupings=['QUEUE'],
        Metrics=[
            {
                'Name': 'CONTACTS_QUEUED',
            },
            {
                'Name': 'AVG_ABANDON_TIME',
            },
            {
                'Name': 'CONTACTS_ABANDONED',
            },
            {
                'Name': 'AVG_HANDLE_TIME',
            },
        ],
        MaxResults=100
    )
    
    for metric_result in response.get('MetricResults', []):
        queue_id = metric_result['Dimensions']['QUEUE']
        for metric_data in metric_result.get('Collections', []):
            metric_name = metric_data.get('Metric', {}).get('Name', 'Unknown')
            try:
                value = metric_data['Value']
            except KeyError:
                value = 0
            data.append([current_date.date(), queue_id, metric_name, value])
    
    current_date = next_date

phone_df_temp = pd.DataFrame(data, columns=['Date','Queue_ID','MetricName', 'Value'])
phone_df_temp.columns = phone_df_temp.columns.str.upper()

def custom_rename(column_name):
    if column_name == "DATE":
        return "Date"
    elif column_name == "METRICNAME":
        return "MetricName"
    elif column_name == "VALUE":
        return "Value"
    elif column_name == "QUEUE_ID":
        return "Queue_ID"
    else:
        return column_name

phone_df = phone_df_temp.rename(columns=custom_rename)

phone_df_with_queue_names = phone_df.merge(queue_df[['Id', 'Name']], left_on='Queue_ID', right_on = 'Id',how='inner')

pivoted_phone_df_with_queue_names = phone_df_with_queue_names.pivot(index=['Date','Name','Queue_ID'], columns='MetricName', values='Value').reset_index()

pivoted_phone_df_with_queue_names['AVG_ABANDON_TIME (mins)'] = (pivoted_phone_df_with_queue_names['AVG_ABANDON_TIME']/60).round(2)

pivoted_phone_df_with_queue_names['ABANDONED_RATE'] = (((pivoted_phone_df_with_queue_names['CONTACTS_ABANDONED'] / pivoted_phone_df_with_queue_names['CONTACTS_QUEUED']) * 100).round(2)).astype(str) + '%'

pivoted_phone_df_with_queue_names['AVG_HANDLE_TIME (mins)'] = (pivoted_phone_df_with_queue_names['AVG_HANDLE_TIME']/60).round(2)

pivoted_phone_df_with_queue_names.drop(columns=pivoted_phone_df_with_queue_names[['AVG_ABANDON_TIME','AVG_HANDLE_TIME','Queue_ID']],inplace=True)

pivoted_phone_df_with_queue_names = pivoted_phone_df_with_queue_names[['Date', 'Name', 'CONTACTS_QUEUED', 'CONTACTS_ABANDONED','ABANDONED_RATE','AVG_ABANDON_TIME (mins)','AVG_HANDLE_TIME (mins)']]

pivoted_phone_df_with_queue_names = pivoted_phone_df_with_queue_names.rename(columns={'Name': 'QUEUE'})

pivoted_phone_df_with_queue_names.columns = pivoted_phone_df_with_queue_names.columns.str.lower()

csv_mappings = {
    'JIRA':df,
    'JIRA Summary':pivot_data,
    'AWS':pivoted_phone_df_with_queue_names}
    
beginning = format(start_date, '%m%d%y')
end = format(end_date, '%m%d%y')
date_string = str(beginning)+'-'+str(end)

excel_buffer = io.BytesIO()

with pd.ExcelWriter(excel_buffer, engine='openpyxl') as excel_writer:
    for sheet_name, dataframe in csv_mappings.items():
        index_param = True if sheet_name == 'JIRA Summary' else False
        dataframe.to_excel(excel_writer, sheet_name=sheet_name, index=index_param)

excel_buffer.seek(0)

attachment_base64 = base64.b64encode(excel_buffer.read()).decode('utf-8')

client_id = graph_client_id
client_secret = graph_secret
tenant_id = graph_tenant_id

url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}
data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://graph.microsoft.com/.default'
}
response = requests.post(url, headers=headers, data=data)
response.raise_for_status()
access_token = response.json().get('access_token')

from_email = 'mdunlap@hhaexchange.com'
to_email = ['jlipson@hhaexchange.com', 'dsweeney@hhaexchange.com','tprause@hhaexchange.com']
subject = 'NC SLA' + ' '+ '-' +' '+ date_string
body = 'NC SLA' + ' '+ '-' + ' '+ date_string

email_recipients = [{"emailAddress": {"address": email}} for email in to_email]

attachment = {
    '@odata.type': '#microsoft.graph.fileAttachment',
    'name': f'NC SLA - {date_string}.xlsx',
    'contentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'contentBytes': attachment_base64
}

send_mail_url = f'https://graph.microsoft.com/v1.0/users/{from_email}/sendMail'
email_msg = {
    'message': {
        'subject': subject,
        'body': {
            'contentType': "Text",
            'content': body
        },
        'toRecipients': email_recipients,
        'attachments': [attachment]
    }
}

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}
response = requests.post(send_mail_url, headers=headers, json=email_msg)
response.raise_for_status()
