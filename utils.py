import os
import json
from zendesk import Zendesk

def get_zd_instance():
    migration_plan = json.load(open('migration_plan.json', 'r'))
    zd_config = migration_plan['zendesk']
    return Zendesk(**zd_config)

def delete_zendesk_tickets(tags:list):
    for idx,result in enumerate(search_tickets_by_tags(tags)):
        print(f'batch {idx}')
        ids = [item['id']  for item in result]
        delete_tickets_by_ids(ids)

def search_tickets_by_tags(tags:list):
    zd = get_zd_instance()
    tags_str = ','.join(map(lambda x:  f'"{x}"',tags))
    results = zd.get(f"/search.json?query=type:ticket tags:{tags_str}", 'results')
    return results

def compile_ticket_mapping_files():
    path = 'migration_data/Case/batches'
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and 'success' in f]
    mapping = {}
    for f in files:
        mapping.update(json.load(open(f,'r')))
    return mapping

def get_tickets_diff():
    results = search_tickets_by_tags(['jive_migrated_ticket'])
    ids = [item['id'] for item in results]
    mapping = compile_ticket_mapping_files()
    difference = set(ids).difference(set(mapping.values()))
    return list(difference)

def delete_tickets_by_ids(ids:list):
    zd = get_zd_instance()

    for i in range(0, len(ids),100):
        batch = ids[i:i+100]
        job_status = zd.delete(f'/tickets/destroy_many.json?ids={",".join(map(str,batch))}', 'job_status')
        if job_status is not None:
            print(f'Bacth {i}:{job_status["status"]}')
        else:
            print(f'Bacth {i}: Failed getting info')

def delete_failed_batch(batch):
    results = search_tickets_by_tags(['jive_migrated_ticket',batch])
    ids = [item['id'] for item in results]
    delete_tickets_by_ids(ids)

def create_mapping_for_batch(batch):
    results = search_tickets_by_tags(['jive_migrated_ticket',batch])
    mapping = {item['external_id']: item['id'] for item in results}
    json.dump(mapping, open(f'migration_data/Case/batches/{batch}','w+'))

def delete_duplicates():
    ids = ['5000b00001WSwcPAAT', '5000b00001WUkd5AAD', '5000b00001WV8m4AAD', '5000b00001S8C3yAAF', '5000b00001WWGe2AAH', '5000b00001WWMgjAAH', '5000b00001WUhQ3AAL', '5000b00001WUfmMAAT', '5000b00001WTyhwAAD', '5000b00001WWMjxAAH', '5000b00001WV8pDAAT', '5000b00001WVk4YAAT', '5000b00001XjzUcAAJ', '5000b00001WV0eTAAT', '5000b00001WV8KEAA1', '5000b00001WUXvVAAX', '5000b00001WUzSqAAL', '5000b00001WWJIYAA5', '5000b00001WVTtEAAX', '5000b00001WW3qIAAT', '5000b00001WSmoLAAT', '5000b00001WVn4MAAT', '5000b00001WV2aJAAT', '5000b00001WVkhGAAT', '5000b00001QfBRgAAN', '5000b00001WTJZfAAP', '5000b00001QjH25AAF', '5000b00001WSkmBAAT', '5000b00001WWHL1AAP', '5000b00001WW2PQAA1', '5000b00001WVicJAAT', '5000b00001Xk6Q9AAJ', '5000b00001WVyt2AAD', '5000b00001SLXpTAAX', '5000b00001WVcPZAA1', '5000b00001WV6jlAAD', '5000b00001WWHvYAAX', '5000b00001WWIB2AAP', '5000b00001WUzSgAAL', '5000b00001WUMAdAAP', '5000b00001VR2ZHAA1', '5000b00001VOZ79AAH', '5000b00001WVjEIAA1', '5000b00001WW2LdAAL', '5000b00001WVrlzAAD', '5000b00001WVwJvAAL', '5000b00001Xk93DAAR', '5000b00001WUEqzAAH', '5000b00001S98LEAAZ', '5000b00001GTIjWAAX', '5000b00001WTkGrAAL', '5000b00001Xk5iCAAR', '5000b00001Xk8fQAAR', '5000b00001WUHHoAAP', '5000b00001WV36PAAT', '5000b00001NjReEAAV', '5000b00001Xk80XAAR', '5000b00001Xk80nAAB', '5000b00001WSc2NAAT', '5000b00001WUhQXAA1', '5000b00001WVYiEAAX', '5000b00001WV6A2AAL', '5000b00001WVFXNAA5', '5000b00001QknOYAAZ', '5000b00001WVwKAAA1', '5000b00001WW7rtAAD', '5000b00001WW1hEAAT', '5000b00001WT3t2AAD', '5000b00001WUo5vAAD', '5000b00001WUkycAAD', '5000b00001WU3tsAAD', '5000b00001WUMNwAAP', '5000b00001WSoYjAAL', '5000b00001S7qBJAAZ', '5000b00001WVjQdAAL', '5000b00001WTRIUAA5', '5000b00001WU1OJAA1', '5000b00001JU5s4AAD', '5000b00001WVl1pAAD', '5000b00001NlATtAAN', '5000b00001WU0bvAAD', '5000b00001WWHijAAH', '5000b00001RIgXdAAL', '5000b00001WUlRUAA1', '5000b00001WVSOKAA5', '5000b00001WVpKNAA1', '5000b00001WWGB5AAP', '5000b00001WWGbIAAX', '5000b00001XjzrqAAB', '5000b00001WV9OSAA1', '5000b00001WVq9LAAT', '5000b00001VPODXAA5', '5000b00001LjIT4AAN', '5000b00001XkFVCAA3', '5000b00001Qh407AAB', '5000b00001WT04JAAT', '5000b00001WVmKOAA1', '5000b00001WUlyTAAT', '5000b00001WUFS6AAP', '5000b00001Xk1LXAAZ']
    zd = get_zd_instance()
    for idx,id in enumerate(ids):
        results = zd.get(f"/tickets?external_id={id}", "tickets")
        if len(results) >1:
            delete_result = zd.delete(f"/tickets/{results[1]['id']}.json")
            if delete_result.status_code == 204:
                print(f'{idx}: deleted{id}')

def retry_import():
    path = f'migration_data/Case/batches'
    zd = get_zd_instance()
    for filename in os.listdir(path):
        if 'error' in filename:
            payload = json.load(open(os.path.join(path, filename), encoding='utf-8-sig'))
            result = zd.post('/imports/tickets/create_many.json',data=payload, response_container= 'job_status')
            print(result)


if __name__ == "__main__":
    retry_import()
    #delete_duplicates()
    #delete_zendesk_tickets(['jive_migrated_ticket'])
#    delete_failed_batch('batch_30')
    #create_mapping_for_batch('batch_30')