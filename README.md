# Salesforce to Zendesk Migration tool

Migrate any object from Salesforce to Zendesk.
Features:
* Google Drive Integration for uploading files;
* Parse rich content fields with inline images;
* Migrate attachments;
* Use custom functions to format/transform data before migrating to Zendesk;
* Create a migration pipeline.

## Pipeline File Structure 
### Credentials section
`` file: migration_plan.json``
```json
{
	"zendesk": {
		"url": "https://<zendesk-domain>.zendesk.com/api/v2",
		"user": <zendesk_username>,
		"password": <zendesk_password>
		},
	"salesforce": {
		"grant_type": "password",
		"client_id": <salesforce_client_id>",
		"client_secret": <salesforce_client_secret>,
		"username": <salesforce_username>,
		"password": <salesforce_password>,
		"cookie_sid":<salesforce_cookie_sid>,
		"cookie_domain": <salesforce_cookie_domain>
		},
	"migration_items": [...]
}
```
For Zendesk, use an administrator account for `zendesk_username` and `zendesk_passoword`.
For Salesforce, follow instruction [here](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/quickstart_oauth.htm) in order to get `salesforce_client_id`, `salesforce_client_secret`.

### Migration Items Section
#### 1) type: migration_object
This type represents a mapping between a Salesforce and a Zendesk object.
```json
{
	"type": "migration_object",
	"sf_object": "Account",
	"zd_object": "organizations",
	"force_download": false,
	"skip": true,
	"after_download": [
						"helpers.build_orgs_hierarchy()"
	],
	"sf_fields": [
					"Id",
					"Name",
					"Description"
	],
	"sf_conditions": [
						"Name like 'My org - %"
	],
	"fields_mapping": [
						{
							"field": {
								"type": "standard",
								"key": "name"
							},
							"value": "'SF - '+ Name",
							"type": "sf_field"
						},
						{
							"field": {
								"type": "standard",
								"key": "external_id"
							},
							"value": "Id",
							"type": "sf_field"
						},
						{
							"field": {
								"type": "standard",
								"key": "notes"
							},
							"value": "Description",
							"type": "sf_field"
						}
	]
}
```

|Key             |Mandatory| Type                          |Description                 |
|----------------|-----------|--------------------|-----------------------------|
|type            |yes|`string`                          |Value: `migration_object`          |
|sf_object       |yes|`string`            |Salesforce object name. (Case sensitve)          |
|zd_object       |yes|`string`|Zendesk objectname. (Case sensitive)|
|force_download  |no|`boolean`|Whether fresh data should be retrieved from Salesforce|
|skip|no|`boolean`|Whether to skip this object|
|after_download|no| `list` of `string` |list of custom functions to be ran after Salesforce download data is completed
|sf_fields|no|`list` of `string` or `sf_object` | List of fields to be downloaded. You can specify an object with `sf_object`, `sf_field` and `sf_conditions` to perform nested queries. If none is provided, all fields will be retrieved. 
|sf_conditions|no|`list` of `string` | List of conditions are concatenated with `AND`
|limit| no | `integer`| Limit result returned by Salesforce. Default: Retrieve all data.
|bulk_create_or_update|no|`boolean`| Whether Zendesk endpoint accepts create/update (upsert operation). Default: `false`.
|upsert|no|`boolean`|Whether to mimic upsert for Zendesk endpoints not supporting such operation. Default: `true`

##### `fields_mapping`
|key| Mandatory| Type| Description|
|-|-|-|-
|field.type|yes|`string`|Acceptable values: `standard, organization_fields, user_fields, custom_fields`
|field.key|yes|`string|integer`|Zendesk field name for standard, organization_fields and user_fields. Id for custom fields.
|type|yes|`string`|Acceptable values: `sf_field`, `mapping` 
|value|yes|`python expression`| Any python expression. For `field_mapping.type`=`sf_field`, all salesforce fields loaded over `migration_item.sf_fields` are mapped to local variables. Dots in Salesforce field names are replaced with an underscore. When using `mapping`, value must be the lookup field name in `source`
|source|yes if type is mapping|`string`|Salesforce object name to lookup values from. The object must be already migrated to Zendesk

#### 2) type: script
|Key             |Mandatory| Type                          |Description                 |
|----------------|-----------|--------------------|-----------------------------|
|type            |yes|`string`                          |Value: `script`      
|skip|no|`boolean`| Whether the execution of this script should be skipped. Default: `False`
|script|yes |python expression|  Any python expression. Fields or migration items entities are not available at this scope.

## Custom functions
Use `helpers.py` to add custom functions that will become available to transform data at every record to be migrated.


## Migration Process

Objects will be migrated in the order they appear in `migration_plan.json`. For every object pulled from Salesforce a folder is created under `migration_data` containing a `csv` and a `json` file with the downloaded data.
After migration is done, a mapping file will be created with the related ids from Zendesk in the same folder.
