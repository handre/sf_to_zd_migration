import json
from zendesk import Zendesk
    
def delete_zendesk_tickets(tags:list):
    migration_plan = json.load(open('migration_plan.json', 'r'))
    zd_config = migration_plan['zendesk']
    zd = Zendesk(**zd_config)
    results = zd.get(f'/search.json?query=type:ticket tags:{",".join(tags)}', 'results')
    ids = [item['id'] for item in results]
    
    for i in range(0, len(ids),100):
        batch = ids[i:i+100]
        job_status = zd.delete(f'/tickets/destroy_many.json?ids={",".join(map(str,batch))}', 'job_status')
        print(f'Bacth {i}:{job_status["status"]}')


if __name__ == "__main__":
    delete_zendesk_tickets(['jive_historical_tickets_migration_test'])