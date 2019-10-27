import os
import log_helper
import random
import csv
import json
import re
import datetime
from gdrive.upload import upload_file

logger = log_helper.get_logger('Helpers')

def tagify(text):
    if text:
        text = "".join([c for c in text.lower() if c in 'abcdefghijklmnopqrstuvwyxz -/0123456789.'])
        text = text.replace(' ','_').replace('-','_').replace('__','_').replace('__','_')
        return text
    return ''

def date_from_str(date_str):
    return datetime.datetime.strptime(date_str,'%Y-%m-%dT%H:%M:%S.%f%z')

def add_seconds_to_date(date, seconds):
    return date + datetime.timedelta(seconds=seconds)

def date_to_str(date):
    return datetime.datetime.strftime(date,'%Y-%m-%dT%H:%M:%SZ')

def process_html_body(migration_item,html_body, comment_id):
    search_result = re.findall(r'\<img.*?src\s*=\s*"(.+?)"', html_body or '')
    urls = [url.replace('&amp;','&') for url in search_result]
    uploads = []
    if len(urls) > 0:
        comment_attachment_path = f'migration_data/Case/Comment_Attachments/{comment_id}'
        os.makedirs(comment_attachment_path,exist_ok=True)

        for idx, url in enumerate(urls):
            
            file_path = comment_attachment_path+f'/{idx}'

            if not os.path.exists(file_path):
                data = migration_item._sf.get_image(url)
                if data[:4] in [b'\x89PNG',b'\xff\xd8\xff\xe0',b'GIF8',b'\xff\xd8\xff\xdb',b'\xff\xd8\xff\xe1']:
                    with open(file_path, 'wb+') as f:
                        f.write(data)     
                
                    response = migration_item._zd.upload_file('sf_rta_image',open(file_path, 'rb'), inline=True)
                    if response.get('attachment',None) is not None:
                        new_url = response['attachment']['content_url']
                        data_upload_id = response['attachment']['id']
                        token = response['token']
                        uploads.append(token)
                        html_body = html_body.replace(url.replace('&',"&amp;"), new_url+ f'" data-imageuploadid="{random.random()}" data-upload-token="{token}" data-inline-attachment="true" data-upload-id="{data_upload_id}')            
                        
    return (html_body, uploads)

def download_cases_attachments(migration_item):
    data = migration_item.get_data()

    for case in data:
        if len(case['Attachment'])>0:
            attachments_path = f'migration_data/Case/Attachments/{case["Id"]}'
            os.makedirs(attachments_path,exist_ok=True)
        
        for attachment in case['Attachment']:
            file_path = f'{attachments_path}/{tagify(attachment["Name"])}'
    
            if not os.path.exists(file_path):
                data  = migration_item._sf.download_attachment(attachment['Id'])
                with open(file_path,'wb+') as f:
                    f.write(data)
    

def build_orgs_hierarchy():
    hierarchy = {}
    data = json.load(open('migration_data/Account/data.json'))

    for org in data:
        hierarchy.update({org['Id']:{'parent_id':[], 'child_id':[]}})
        if org['Parent.Id']:
            hierarchy[org['Id']]['parent_id'].append(org['Parent.Id'])

        for iorg in data:
            if iorg['Parent.Id'] == org['Id']:
                if org['Id'] not in hierarchy[org['Id']]['child_id']:
                    hierarchy[org['Id']]['child_id'].append(iorg['Id'])
        
        for iorg in data:
            if iorg['Id'] in hierarchy[org['Id']]['child_id'] and iorg['Parent.Id']:
                if iorg['Parent.Id'] not in hierarchy[org['Id']]['parent_id'] and iorg['Parent.Id'] != org['Id']:
                    hierarchy[org['Id']]['parent_id'].append(iorg['Parent.Id'])
 
    json.dump(hierarchy, open('migration_data/Account/hierarchy_data.json','w+'))

def get_hierarchy_text(org_id):
    def get_org_name(org_id):
        data = json.load(open('migration_data/Account/data.json','r'))
        org = [org for org in data if org['Id']==org_id]
        if len(org)==1:
            return org[0]['Name']

    hierarchy = get_hierarchy_info(org_id)
    translated = {}
    for key, value in hierarchy.items():
        translated.update({get_org_name(key):[get_org_name(child) for child in value]})

    text = ''
    for key,values in translated.items():
        text += f'• {key}\n'
        for value in values:
            text+=f'\t• {value}\n'
    return 'No Child Orgs' if text == '' else text

def get_child_accounts(org_id):
    hierarchy = get_hierarchy_info(org_id)
    children = []
    for key, values in hierarchy.items():
        children.append(key)
        for value in values:
            children.append(value)
    return children

def get_hierarchy_info(org_id):    
    data = json.load(open('migration_data/Account/hierarchy_data.json','r'))
    hierarchy = {}

    org = data.get(org_id,None)
    if org:
        if len(org['child_id'])>0:
            hierarchy = {child:[] for child in org['child_id']}
            for key,_ in hierarchy.items():
                if len(data[key]['child_id']) > 0:
                    hierarchy[key] = data[key]['child_id']
    return hierarchy

def sf_to_zd_orgs(orgs,env='dev'):
    data = json.load(open(f'migration_data/Account/{env}-mapping.json','r'))
    return [zd_org for sf_org, zd_org in data.items() if sf_org in orgs]

def create_users_membership_payload(env='dev'):
    users_data = json.load(open('migration_data/User/data.json','r',encoding='utf-8-sig'))
    users_mapping = json.load(open(f'migration_data/User/{env}-mapping.json','r'))

    payload = {'organization_memberships':[]}

    for user in users_data:
        child_orgs = get_child_accounts(user['AccountId'])
        if len(child_orgs) > 0 and user['UserRole.Name'].lower().find('manager')>=0:
            zd_user_id = users_mapping[user['Id']]
            zd_orgs = sf_to_zd_orgs(child_orgs,env)
        
            for org_id in zd_orgs:
                membership = {'user_id':zd_user_id, 'organization_id':org_id, 'default':False}
                payload['organization_memberships'].append(membership)    

    json.dump(payload, open('migration_data/User/org_membership.json','w+'))

def create_attachments_comment(migration_item,fields):
    if len(fields['Attachment'])>0:
        created_at = date_from_str(fields['CreatedDate'])
        token = None
        html_body = 'Migrated Attachments'
        over20 = False
        for attachment in fields['Attachment']:
            if attachment['BodyLength'] > 0:
                file_path = f"migration_data/Case/Attachments/{fields['Id']}/{tagify(attachment['Name'])}"
                
                if attachment['BodyLength'] in range(1,20000000):
                    response = migration_item._zd.upload_file(tagify(attachment['Name']),open(file_path,'rb'), token)
                    token = response.get('token',None)
                    if token is None:
                        logger.critical(f'Could not upload File: {tagify(attachment["Name"])}\tSize:{attachment["BodyLength"]}\tId{fields["Id"]}')
                else:
                    over20 = True
                    link = upload_file(file_path)
                    html_body += f'<br><a href="{link}">{tagify(attachment["Name"])}</a>'

        if over20:
            html_body +="<br><i><b>Note:</b> Attachments over 20MB are stored at Trilogy's Google Drive</i>"
        
        return {
                "public":True, 
                "html_body": html_body,
                "created_at": date_to_str(add_seconds_to_date(created_at,1)),
                "uploads":[token]}
    return None

def make_comments(migration_item, fields):
    comments = []
    standard_comments = [
        {
            'comment':item['CommentBody'], 
            'created_by': item['CreatedById'],
            'created_date':item['CreatedDate'],
            'public': item['IsPublished'],
            'id': item['Id']
        }
        for item in fields['CaseComment']]

    users_mapping = migration_item._get_mapping('User')

    for case_comment in standard_comments:
        author_id = users_mapping.get(case_comment['created_by'], None)
        public = case_comment['public']
        comment_created_at = date_from_str(case_comment['created_date'])
        html_body, uploads = process_html_body(migration_item,case_comment['comment'], case_comment['id'])
        comment = {
            "html_body":html_body or '[No Content]',
            "public": public,
            "uploads":uploads,
            "created_at":date_to_str(comment_created_at)}

        if author_id:
            comment.update({'author_id':author_id})
        comments.append(comment)

    return comments

def create_comments_payload(migration_item, **fields):

    payload_path = f"migration_data/Case/Comment_payload/{fields['Id']}_comments_payload.json"

    if os.path.exists(payload_path):
        return json.load(open(payload_path,'r',encoding='utf-8-sig'))

    comments = []


    attachments_comment = create_attachments_comment(migration_item, fields)
    if attachments_comment:
        comments.append(attachments_comment)
    
    std_comments = make_comments(migration_item, fields)
    if std_comments:
        comments.extend(std_comments)
            
    json.dump(comments,open(payload_path,'w+',encoding='utf-8-sig'))

    return comments