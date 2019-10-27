import os
import logging
import time

import csv
import json
from string import Formatter
import helpers
from log_helper import get_logger

class MigrationItem():
    log = get_logger('MigrationPlan')
    __data_folder__ = 'migration_data'

    def __init__(self, sf, zd, **kwargs):
        self.env_mode = kwargs.get('mode','dev')

        self._sf = sf
        self._zd = zd

        self.zd_custom_endpoint = kwargs.get('zd_custom_endpoint',None)
        
        self.skip = kwargs.get('skip', True)
        self.sf_object = kwargs.get('sf_object',None)
        self.payload_file = kwargs.get('file',None)
        self.zd_object = kwargs['zd_object']
        self.limit = kwargs.get('limit',None)      
        self.upsert = kwargs.get('upsert', True)        
        self.create_mapping = kwargs.get('create_mapping',True)
        self.after_download = kwargs.get('after_download',[])

        self.bulk_create_or_update = kwargs.get('bulk_create_or_update', False)
        
        self.sf_fields = [
            field for field in kwargs.get('sf_fields', []) if type(field) == str]
        
        self.sf_join_fields = [
            field for field in kwargs.get('sf_fields', []) if type(field) == dict]
        
        if len(self.sf_fields) == 0 and self.sf_object:
            self.sf_fields = [field['name']
                              for field in sf.discover_fields(self.sf_object)]
        
        self.sf_conditions = kwargs.get('sf_conditions', [])
        self.fields_mapping = kwargs.get('fields_mapping', [])

        self.force_download = kwargs.get('force_download', True)

        self.export_file = f'{self.__data_folder__}/{self.sf_object}/data.csv'
        self.data_file = f'{self.__data_folder__}/{self.sf_object}/data.json'
        self.id_mapping_file = f'{self.__data_folder__}/{self.sf_object}/{self.env_mode}-mapping.json'
        self.errors_file = f'{self.__data_folder__}/{self.sf_object or self.zd_object}/errors.json'

    def __build_directory(self):
        os.makedirs(f'{self.__data_folder__}/{self.sf_object or self.zd_object}', exist_ok=True)

    def __parse_sf_row(self, row, fields=None):
        parsed_row = {}

        for field in (fields or self.sf_fields):
            if len(field.split('.')) == 1:
                value = row[field]
            elif row[field.split('.')[0]] is not None:
                field_key = field.split('.')[0]
                child_key = field.split('.')[-1]
                value = row[field_key].get(child_key, None)
            else:
                value = None
            parsed_row.update({field: value})

        return parsed_row

    def get_data(self):
        if self.sf_object is None and self.payload_file:
            data = json.load(open(self.payload_file, 'r', encoding='utf-8-sig'))
        else:
            if os.path.exists(self.data_file) is False:
                self.download_data()

            data = json.load(open(self.data_file, 'r', encoding='utf-8-sig'))

        return data

    def download_data(self):
        self.__build_directory()

        self.log.info(
            f'Downloading data from SalesForce for {self.sf_object} object')

        csv_writer = csv.DictWriter(open(
            self.export_file, 'w+', newline='', encoding="utf-8-sig"), self.sf_fields, extrasaction='ignore')
        csv_writer.writeheader()

        data = []
        for rows in self._sf.query_all(self.sf_object, [field for field in self.sf_fields if type(field) == str], self.sf_conditions, limit=self.limit):
            if len(rows) > 0:
                for row in rows:
                    parsed_row = self.__parse_sf_row(row)

                    for join_field in self.sf_join_fields:
                        self.log.info(f'Getting {join_field["sf_object"]} for {self.sf_object}:{row["Id"]} object')
                        parsed_row.update({join_field['sf_object']:[]})
                        for join_rows in self._sf.query_all(join_field['sf_object'], join_field['sf_fields'], [
                                                       eval(condition,{'row':row}) for condition in join_field['sf_conditions']]):
                            for join_row in join_rows:
                                parsed_row[join_field['sf_object']].append( self.__parse_sf_row(join_row,join_field['sf_fields']))
                    data.append(parsed_row)
                    csv_writer.writerow(parsed_row)
        json.dump(data,open(self.data_file,'w+'))
        self.log.info('Done.')

        self.on_after_download()

    def on_after_download(self):
        for func in self.after_download:
            self.log.info(f'Evaluating function after download {func}')
            eval(func)

    def _update_zd_obj(self, json):
        result = self._zd.put(
            f'/{self.zd_custom_endpoint or self.zd_object}/update_many.json', json=json, response_container='job_status')
        status = result['status']

        while status in ['queued', 'working']:
            job_url = result['url']
            result = self._zd.get(job_url, 'job_status')
            status = result['status']
            if status in ['queued', 'working']:
                time.sleep(10)

        return result.get('results', None)

    def _create_zd_obj(self, json):
        result = self._zd.post(
            f'/{self.zd_custom_endpoint or self.zd_object}/create_many.json', json=json, response_container='job_status')
        status = result['status']

        while status in ['queued', 'working']:
            job_url = result['url']
            result = self._zd.get(job_url, 'job_status')
            status = result['status']
            if status in ['queued', 'working']:
                message = result.get('message',None)
                if message:
                    self.log.info(message)
                else:
                    self.log.info('Waiting reponse...')
                time.sleep(10)

        return result.get('results', None)

    def _create_or_update_zd_obj(self, json):
        result = self._zd.post(
            f'/{self.zd_custom_endpoint or self.zd_object}/create_or_update_many.json', json=json, response_container='job_status')
        status = result['status']

        while status in ['queued', 'working']:
            job_url = result['url']
            result = self._zd.get(job_url, 'job_status')
            status = result['status']
            if status in ['queued', 'working']:
                self.log.info('Waiting reponse...')
                time.sleep(10)

        return result.get('results', None)

    def get_zd_payload(self):
        self.log.info(f'Building Zendesk {self.zd_object} object payload')
        data = self.get_data()
        total = len(data)

        if self.sf_object:        
            payload = {self.zd_object: []}
            
            for idx, fields in enumerate(data):
                self.log.info(f'Processing... {idx}/{total}')
                mapped = {}
                for map_item in self.fields_mapping:
                    field_type = map_item['field']['type']
                    field_key = map_item['field']['key']
                    field_value = map_item['value']
                    value_type = map_item.get('type',None)
                    if value_type == 'sf_field':
                        field_value = eval(field_value,{"helpers":helpers, "self":self},{key.replace('.','_'):value for key,value in fields.items()})

                    elif value_type == 'mapping':
                        mapping_source = map_item.get('source', None)
                        mapping_key = field_value
                        if mapping_source and mapping_key:
                            mapping = self._get_mapping(mapping_source)
                            if mapping:
                                field_value = mapping.get(
                                    fields[mapping_key], None)
                                if field_value is None:
                                    self.log.warning(
                                        f'No Mapping found for {field_key} in {mapping_source} |\tkey:{fields[mapping_key]}')
                            else:
                                fallback_value = map_item.get('fallback_value',None)
                                field_value = fallback_value
                                self.log.warning(
                                    f'No Mapping found for {field_key} in {mapping_source}. Fallbak value is used instead')
                    else:
                        field_value = map_item['value']

                    if field_value is not None:
                        if field_type == 'standard':
                            mapped.update({field_key: field_value})
                        else:
                            if mapped.get(field_type, None):
                                mapped[field_type].update({field_key: field_value})
                            else:
                                mapped.update(
                                    {field_type: {field_key: field_value}})
                payload[self.zd_object].append(mapped)

            self.log.info(f'Done.')

            return payload
        return data

    def _create_batch_payload(self):
        batches = []
        payload = self.get_zd_payload()

        slices = round(len(payload[self.zd_object])/100+0.5)
        slice_size = 100
        for i in range(slices):
            batches.append(
                {self.zd_object: payload[self.zd_object][i*slice_size:slice_size + i*slice_size]})
        return batches

    def _create_batch_list(self, items):
        batches = []

        slices = round(len(items)/100+0.5)
        slice_size = 100
        for i in range(slices):
            batches.append(
                list(items)[i*slice_size: slice_size + i*slice_size])
        return batches

    def _get_mapping(self, source=None):
        if source:
            file = f'{self.__data_folder__}/{source}/{self.env_mode}-mapping.json'
            return self.__get_json_file(file)
        if os.path.exists(self.id_mapping_file):
            return self.__get_json_file(self.id_mapping_file)
        return {}

    def __get_json_file(self, file):
        if os.path.exists(file):
            f = open(file, 'r')
            data = f.read()
            f.close()
            return json.loads(data)
        return None

    def get_sync_diff(self):
        mapping = self._get_mapping()
        if mapping:
            data = self.get_data()
            new_obj_ids = set([item['Id'] for item in data])
            old_obj_ids = set([key for key in mapping.keys()])
            diff = old_obj_ids.difference(new_obj_ids)
            return diff
        return None

    def migrate(self):
        self.__build_directory()
        
        self.log.info(f'Starting Migration of {self.zd_object} Object')

        if self.sf_object:
            if self.force_download == True:
                self.download_data()


        payload_batches = self._create_batch_payload()
        json.dump(payload_batches,open(f'migration_data/{self.sf_object}/import_payload.json','w+'))

        obj_mapping = {}
        failed_obj = {self.zd_object: []}
        current_batch = 1
        self.log.info(f'Uploading data to zendesk')

        for batch in payload_batches:
            batch_success = len(batch[self.zd_object])
            batch_failed = 0
            self.log.info(f'\tBatch {current_batch} of {len(payload_batches)}')

            if self.bulk_create_or_update == True:
                value = batch
                key = 'bulk_create_update'
                result = self._create_or_update_zd_obj(batch)

                if result:
                    for i in range(len(value[self.zd_object])):
                        id = result[i].get('id', None)
                        if id:
                            if self.create_mapping == True:
                                obj_mapping.update(
                                    {value[self.zd_object][i]['external_id']: id})
                            self.log.info(f"[{self.zd_object}][{key}] SalesForce Id: {value[self.zd_object][i]['external_id']}\tZendesk Id:{id}")
                        else:
                            if key != 'create':
                                obj_mapping.update({value[self.zd_object][i]['external_id']: value[self.zd_object][i]['external_id']})
                            item = value[self.zd_object][i]
                            item.update({'_log': result[i]})
                            failed_obj[self.zd_object].append(item)
                            batch_failed += 1
            else:
                if self.upsert:
                    mapping = self._get_mapping()
                    update_batch = {self.zd_object:[item for item in batch[self.zd_object] if item.get('external_id','') in mapping.keys()]}
                    create_batch = {self.zd_object:[item for item in batch[self.zd_object] if item.get('external_id','') not in mapping.keys()]}
                    batch_operation = {'update':update_batch, 'create':create_batch}
                else:
                    batch_operation = {'create': batch }

                for key, value in batch_operation.items():
                    result = None
                    if len(value[self.zd_object]):
                        if key == 'update':
                            result = self._update_zd_obj(value)
                        else:
                            result = self._create_zd_obj(value)

                    if result:
                        for i in range(len(value[self.zd_object])):
                            id = result[i].get('id', None)
                            if id:
                                if self.create_mapping == True:
                                    obj_mapping.update(
                                        {value[self.zd_object][i]['external_id']: id})
                                    self.log.info(f"[{self.zd_object}][{key}] SalesForce Id: {value[self.zd_object][i]['external_id']}\tZendesk Id:{id}")
                            else:
                                if key == 'update':
                                    obj_mapping.update({value[self.zd_object][i]['external_id']: value[self.zd_object][i]['external_id']})
                                item = value[self.zd_object][i]
                                item.update({'_log': result[i]})
                                failed_obj[self.zd_object].append(item)
                                batch_failed += 1
                        
                    
                                
            current_batch += 1
            batch_success -= batch_failed
            self.log.info(f'\tBatch completed.')
            self.log.info(f'\t\tSuccess:\t {batch_success}')
            self.log.info(f'\t\tFailed :\t {batch_failed}. Check Error Log file.')
        self.log.info(f'All batches completed.')
        if self.create_mapping == True:
            json.dump(obj_mapping, open(self.id_mapping_file, 'w+'))
        json.dump(failed_obj, open(self.errors_file, 'w+'))
