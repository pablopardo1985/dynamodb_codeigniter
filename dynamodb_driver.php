<?php

defined('BASEPATH') OR exit('No direct script access allowed');

require_once __DIR__.'/../../../libraries/aws/vendor/autoload.php';

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;

class CI_DB_dynamodb_driver {
	public $dbdriver = 'dynamodb';
	protected $_dynamodb;
	protected $_sdk;
	protected $_marshaler;
	protected $_region;
	protected $_version;
	protected $_key;
	protected $_secret;

	public function __construct($params) {

		$this->_region = $params["region"];
		$this->_version = $params["version"];
		$this->_key = $params["key"];
		$this->_secret = $params["secret"];
	}

	public function initialize() {
		$this->_sdk = new Aws\Sdk([
			'region' => $this->_region,
			'version' => $this->_version,
			'credentials' => [
				'key' => $this->_key,
				'secret' => $this->_secret
			]
		]);

		$this->_dynamodb = $this->_sdk->createDynamoDb();
		$this->_marshaler = new Marshaler();

		return $this->_dynamodb;
	}

	protected function _returnData($status, $data, $params, $function) {
		$response = [
			"status" => $status,
			"message" => $function,
			"request" => $params,
			"count" => 0,
			"items" => []
		];

		if ($status) {
			if (isset($data["Items"]) && is_array($data["Items"])) {
				//SEVERAL ITEMS
				$return["Count"] = $data['Count'];

				foreach ($data['Items'] as $k => $item) {
					foreach ($item as $q => $val) {
						$return['Items'][$k][$q] = $this->_marshaler->unmarshalValue($val);
					}
				}
			} elseif ($data["Item"]) {

				//ONE ITEM
				$return["Count"] = 1;

				foreach ($data["Item"] as $k => $v) {
					$return['Items'][0][$k] = $this->_marshaler->unmarshalValue($v);
				}
			} else {
				$return["Count"] = 0;
				$return["Items"] = [];
			}

			$response["count"] = $return["Count"];
			$response["items"] = $return["Items"];
		}

		return $response;
	}

	public function createTable($table, $keys, $attributes) {
		$params = [
			'TableName' => $table,
			'KeySchema' => $keys,
			'AttributeDefinitions' => $attributes
		];

		try {
			$execute = $this->_dynamodb->createTable($params);

			return $this->_returnData(true, $execute, $params, "CREATE TABLE OK");
		} catch (DynamoDbException $e) {
			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}

	public function putItem($table, $item) {

		$params = [
			'TableName' => $table,
			'Item' => $this->_marshaler->marshalJson(json_encode($item))
		];

		try {
			$execute = $this->_dynamodb->putItem($params);

			return $this->_returnData(true, $execute, $params, "PUT ITEM OK");
		} catch (DynamoDbException $e) {

			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}

	public function getItem($table, $key) {
		$params = [
			'TableName' => $table,
			'Key' => $this->_marshaler->marshalJson(json_encode($key))
		];

		try {
			$execute = $this->_dynamodb->getItem($params);

			return $this->_returnData(true, $execute, $params, "GET ITEM OK");
		} catch (DynamoDbException $e) {

			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}

	public function updateItem($table, $key, $operation, $values, $condition = "") {

		$expression = [];
		$values = [];
		$counter = 0;

		foreach ($values as $k => $v) {
			$expression[] = $k." = :v".$counter;
			$values[] = [":v".$counter => $v];
			$counter++;
		}

		$params = [
			'TableName' => $table,
			'Key' => $key,
			'UpdateExpression' => $operation.' '.implode(", ", $expressions),
			'ExpressionAttributeValues' => $this->_marshaler->marshalJson(json_encode($values)),
			'ReturnValues' => 'UPDATED_NEW'
		];

		if ($condition != "") {
			$params['ConditionExpression'] = $condition;
		}

		try {
			$execute = $this->_dynamodb->updateItem($params);

			return $this->_returnData(true, $execute, $params, "UPDATE ITEM OK");
		} catch (DynamoDbException $e) {

			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}

	public function query($table, $expression, $values, $FilterExpression, $ProjectionExpression = "") {

		$params = [
			'TableName' => $table,
			'KeyConditionExpression' => $expression,
			'ExpressionAttributeValues' => $this->_marshaler->marshalJson(json_encode($values))
		];

		if ($FilterExpression != "") {
			$params["FilterExpression"] = $FilterExpression;
		}

		if ($ProjectionExpression != "") {
			$params["ProjectionExpression"] = $ProjectionExpression;
		}

		try {
			$execute = $this->_dynamodb->query($params);

			return $this->_returnData(true, $execute, $params, "QUERY OK");
		} catch (DynamoDbException $e) {

			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}

	public function scan($table, $expression, $values, $ProjectionExpression = "") {

		$params = [
			'TableName' => $table,
			'FilterExpression' => $expression,
			'ExpressionAttributeValues' => $this->_marshaler->marshalJson(json_encode($values))
		];

		if ($ProjectionExpression != "") {
			$params["ProjectionExpression"] = $ProjectionExpression;
		}

		try {
			$execute = $this->_dynamodb->scan($params);

			return $this->_returnData(true, $execute, $params, "SCAN OK");
		} catch (DynamoDbException $e) {

			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}

	public function deleteTable($table) {

		$params = [
			'TableName' => $table
		];

		try {
			$execute = $this->_dynamodb->deleteTable($params);

			return $this->_returnData(true, $execute, $params, "DELETE TABLE OK");
		} catch (DynamoDbException $e) {

			return $this->_returnData(false, [], $params, $e->getMessage());
		}
	}
}
