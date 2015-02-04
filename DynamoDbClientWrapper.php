<?php namespace EgbertWietses\DynamoDbWrapper;

use Aws\DynamoDb\DynamoDbClient;

/**
 * Class DynamoDbClientWrapper
 */
class DynamoDbClientWrapper {

	private $client;

	/**
	 * @param $region
	 * @param $key
	 * @param $secret
	 */
	public function __construct($region, $key, $secret)
	{
		$this->client = DynamoDbClient::factory(array(
			'region'	=> $region,
			'key'		=> $key,
			'secret'	=> $secret
		));
	}
}