import json
import csv
import os
from config import ENV
from salesforce import SalesForce
from zendesk import Zendesk
from migration import MigrationItem
from log_helper import get_logger
import helpers

log = get_logger('Main')

if __name__ == "__main__":
    migration_plan = json.load(open('migration_plan.json', 'r'))
    sf_config = migration_plan['salesforce']
    sf = SalesForce(**sf_config)
    
    zd_config = migration_plan['zendesk']
    zd = Zendesk(**zd_config)

    for item in migration_plan['migration_items']:
        if item['skip'] == False:
            if item['type'] == 'migration_object':
                migration_item = MigrationItem(sf, zd, mode=ENV, **item)
                if migration_item.skip == False:
                    try:
                        migration_item.migrate()
                    except Exception as err:
                        log.critical(err)
                        raise err
            elif item['type'] == 'script':
                eval(item['script'],{"helpers":helpers, "env":ENV})

