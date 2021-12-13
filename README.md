# dynamodb_codeigniter
Small DB Driver for CodeIgniter v3, that allows the use of DynamoDB instead SQL database

Just download the AWS PHP SDK and extract it into system/libraries.

In application/config/database.php add the following:


```
$db['default'] = array(
	'key' => 'YOUR_AWS_KEY',
	'region' => 'YOUR_AWS_REGION',
	'secret' => 'YOUR_AWS_SECRET',
	'dbdriver' => 'dynamodb',
	'version' => 'latest'
);
```
