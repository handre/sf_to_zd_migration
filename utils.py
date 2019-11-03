import os
import json
from zendesk import Zendesk

def get_zd_instance():
    migration_plan = json.load(open('migration_plan.json', 'r'))
    zd_config = migration_plan['zendesk']
    return Zendesk(**zd_config)

def delete_zendesk_tickets(tags:list):
    ids = search_tickets_by_tags(tags)
    delete_tickets_by_ids(ids)

def search_tickets_by_tags(tags:list):
    zd = get_zd_instance()
    results = zd.get(f'/search.json?query=type:ticket tags:{",".join(tags)}', 'results')
    ids = [item['id'] for item in results]
    return ids

def compile_ticket_mapping_files():
    path = 'migration_data/Case/batches'
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and 'success' in f]
    mapping = {}
    for f in files:
        mapping.update(json.load(open(f,'r')))
    return mapping

def get_tickets_diff():
    ids = search_tickets_by_tags(['jive_migrated_ticket'])
    mapping = compile_ticket_mapping_files()
    difference = set(ids).difference(set(mapping.values()))
    return list(difference)

def delete_tickets_by_ids(ids:list):
    zd = get_zd_instance()

    for i in range(0, len(ids),100):
        batch = ids[i:i+100]
        job_status = zd.delete(f'/tickets/destroy_many.json?ids={",".join(map(str,batch))}', 'job_status')
        print(f'Bacth {i}:{job_status["status"]}')

def delete_failed_batch():
    ids = get_tickets_diff()
    delete_tickets_by_ids(ids)

if __name__ == "__main__":
    #delete_zendesk_tickets(['jive_migrated_ticket'])
    delete_failed_batch()