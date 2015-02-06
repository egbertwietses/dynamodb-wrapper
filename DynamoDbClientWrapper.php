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
        
        public function emptyTable($table) {
                // Get table info
                $result = $this->client->describeTable(array('TableName' => $table));
                $keySchema = $result['Table']['KeySchema'];

                foreach ($keySchema as $schema) {
                        if ($schema['KeyType'] === 'HASH') {
                            $hashKeyName = $schema['AttributeName'];
                        }
                        else if ($schema['KeyType'] === 'RANGE') {
                            $rangeKeyName = $schema['AttributeName'];
                        }
                }
                // Delete items in the table
                $scan = $this->client->getIterator('Scan', array('TableName' => $table));
                foreach ($scan as $item) {
                        // set hash key
                        $hashKeyType = array_key_exists('S', $item[$hashKeyName]) ? 'S' : 'N';
                        $key = array(
                            $hashKeyName => array($hashKeyType => $item[$hashKeyName][$hashKeyType]),
                        );
                        // set range key if defined
                        if (isset($rangeKeyName)) {
                            $rangeKeyType = array_key_exists('S', $item[$rangeKeyName]) ? 'S' : 'N';
                            $key[$rangeKeyName] = array($rangeKeyType => $item[$rangeKeyName][$rangeKeyType]);
                        }
                        $this->client->deleteItem(array(
                            'TableName' => $table,
                            'Key' => $key
                        ));
                }
        }        
}