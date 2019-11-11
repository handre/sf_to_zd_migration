import requests
from log_helper import get_logger

class SalesForce():
    log = get_logger('SalesForce')
    __auth_params__ = set(['grant_type', 'client_id', 'client_secret', 'username', 'password'])
    _service_path = '/services/data/v40.0'

    def __init__(self, **kwargs):
        auth_params = {}

        if self.__auth_params__.issubset(set(kwargs.keys())):
            for param in self.__auth_params__:
                auth_params.update({param : kwargs.get(param, None)})
            self.session = requests.Session()
            self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'})
            r = self.session.post("https://login.salesforce.com/services/oauth2/token", params=auth_params)
            self._access_token = r.json().get("access_token",None)
            self.session.headers['Authorization'] = f'Bearer {self._access_token}'
            self.session.cookies.set(name='sid',value=kwargs['cookie_sid'], domain=kwargs['cookie_domain'])
            
            self.instance_url = r.json().get("instance_url",None)
            
    def get_image(self,url):   
        response = self.session.get(url, timeout=(2,5))
        return response.content

    def _api_call(self, action, parameters = {}, method = 'get', data = {}):
        """
        Helper function to make calls to Salesforce REST API.
        Parameters: action (the URL), URL params, method (get, post or patch), data for POST/PATCH.
        """
        headers = {
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
            'Authorization': 'Bearer %s' % self._access_token
        }
        if self._service_path not in action:
            action = self._service_path+action
        if method == 'get':
            r = self.session.request(method, self.instance_url+action, headers=headers, params=parameters)
        elif method in ['post', 'patch']:
            r = self.session.request(method, self.instance_url+action, headers=headers, json=data, params=parameters)
        else:
            # other methods not implemented in this example
            raise ValueError('Method should be get or post or patch.')
        self.log.debug('Debug: API %s call: %s' % (method, r.url) )
        if r.status_code < 300:
            if method=='patch':
                return None
            else:
                if 'application/json' in  r.headers.get('content-type'):
                    return r.json()
                else:
                    return r.content
        else:
            raise Exception('API error when calling %s : %s' % (r.url, r.content))

    def discover_fields(self, sf_object):
        response = self._api_call(f'/sobjects/{sf_object}/describe')

        if not response.get('queryable', False):
            raise Exception("This object is not queryable")

        return response.get('fields',None)
    
    def get_case_comments(self, case_id):
        fields = ['CreatedById', 'CreatedDate','CommentBody','IsPublished']
        conditions = [f'ParentId={case_id}']
        response = self.query_all('CaseComments',fields, conditions)
        return response
    
    def get_case_attachments(self, case_id):
        response = self._api_call(f'/Case/{case_id}/Attachments')
        return response
    
    def download_attachment(self, file_id):
        response = self._api_call(f'/sobjects/Attachment/{file_id}/body')
        return response

    def query_all(self, sf_object, fields, conditions=[], limit=None):

        query_str = f"SELECT {', '.join(fields)} FROM {sf_object} "+ ("WHERE " + " AND ".join(conditions) if len(conditions)>0 else "")+(f"LIMIT {limit}" if limit else "" )
        
        response = self._api_call('/queryAll/', {'q': query_str})
        next = response.get('nextRecordsUrl', None)
        yield response.get('records', [])
        
        while next:
            response = self._api_call(next)
            next = response.get('nextRecordsUrl', None)
            yield response.get('records', [])
    
    