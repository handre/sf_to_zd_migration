import requests
import json
import time

class Zendesk():
    def __init__(self, **kwargs):
        self.url= kwargs.get('url', None)
        self._auth = (kwargs.get('user',None), kwargs.get('password'))

    def get(self,path, response_container=None):
        if self.url in path:
            next_url = path
        else:
            next_url = self.url + path
            
        items = []
        while next_url:
            r = requests.get(next_url, auth=self._auth)
            if r.status_code == 200:
                result = r.json()
                next_url = result.get('next_page',None)
                data = result[response_container] if response_container else result
                #yield data
                if type(data) == list:
                    items += data
                elif next_url is None and type(data) == dict and len(items) == 0:
                    items = data
                else:
                    items.append(data)


            if r.status_code == 429:
                wait_for =int(r.headers['Retry-After'])
                time.sleep(wait_for)

        return items
    
    def upload_file(self, filename, data, token=None, inline=False):
        url = self.url + f'/uploads.json?filename={filename}{"&token="+token if token else ""}{"&inline=true" if inline else ""}'
                
        r = requests.post(url, auth=self._auth, headers={'Content-Type':'application/binary'},data=data)
        if r.status_code == 201:
            return r.json()['upload']
        return {'status_code':r.status_code}
        
    def post(self,path, data, response_container=None):
        url = self.url + path

        r = requests.post(url,auth=self._auth, json=data)

        if r.status_code in [200, 201]:
            return r.json().get(response_container, None) if response_container else r
        return r

    def put(self,path, json, response_container=None):
        url = self.url + path

        r = requests.put(url, auth=self._auth, json=json)

        if r.status_code in [200, 201]:
            return r.json().get(response_container, None) if response_container else r
        return r

    def delete(self,path,response_container=None):
        url = self.url + path
        r = requests.delete(url, auth=self._auth)

        return r.json().get(response_container, None) if response_container else r

    def check_job_status(self,ref,response):
        job_status_url = response['url']
        status = response['status']
        print('Request enqueued')

        while status not in ['completed', 'failed']:
            print('Checking...')
            
            response = self.get(job_status_url,'job_status')
            if response:
                results = response['results']
                status = response['status']
                for result in results:
                    if result.get('error', None):
                        index = result['index']
                        print(f"{ref[index]['user_id']}\t::\t{result['details'] if result.get('details',None) else result.get('error',result)}")
                print(response['message'])