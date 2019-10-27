import json
import os
from zendesk import Zendesk
from config import global_config, ENV

def undo_last_ticket_migration():
    mapping_path = 'migration_data/Case/prod-mapping.json'
    mapping = json.load(open(mapping_path,'r', encoding='utf-8-sig'))

    ids = [str(value) for _,value in mapping.items()]

    batches=[]
    slices = round(len(ids)/100+0.5)
    slice_size = 100

    for i in range(slices):
        batches.append(ids[i*slice_size:slice_size + i*slice_size])

    zd_config = global_config['zendesk']
    zd = Zendesk(**zd_config)
    for batch in batches:
        if len(batch) >0:
            path = f'/tickets/destroy_many.json?ids={",".join(batch)}'
            response = zd.delete(path,'job_status')
            print(response)
    os.remove(mapping_path)
    print('Scheduled for deleting')

if __name__ == "__main__":
    undo_last_ticket_migration()