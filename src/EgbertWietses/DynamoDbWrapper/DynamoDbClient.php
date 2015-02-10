<?php namespace EgbertWietses\DynamoDbWrapper;

/**
 * Class DynamoDbClient
 */
class DynamoDbClient {

    private $client;

    /**
     * @param $region
     * @param $key
     * @param $secret
     */
    public function __construct($region, $key, $secret)
    {
        $this->client = \Aws\DynamoDb\DynamoDbClient::factory([
            'region' => $region,
            'key'    => $key,
            'secret' => $secret
        ]);
    }

    /**
     * @param $tableName
     * @param $fields
     * @throws \Exception
     */
    public function putItem($tableName, $fields)
    {
        try
        {
            $response = $this->client->putItem([
                "TableName"              => $tableName,
                "Item"                   => $fields,
                "ReturnConsumedCapacity" => "TOTAL"
            ]);
        }
        catch (\Exception $ex)
        {
            throw $ex;
        }
    }

    public function emptyTable($table)
    {
        // Get table info
        $result = $this->client->describeTable(['TableName' => $table]);
        $keySchema = $result['Table']['KeySchema'];

        foreach ($keySchema as $schema)
        {
            if($schema['KeyType'] === 'HASH')
            {
                $hashKeyName = $schema['AttributeName'];
            }
            else if($schema['KeyType'] === 'RANGE')
            {
                $rangeKeyName = $schema['AttributeName'];
            }
        }
        // Delete items in the table
        $scan = $this->client->getIterator('Scan', ['TableName' => $table]);
        foreach ($scan as $item)
        {
            // set hash key
            $hashKeyType = array_key_exists('S', $item[$hashKeyName]) ? 'S' : 'N';
            $key = [
                $hashKeyName => [$hashKeyType => $item[$hashKeyName][$hashKeyType]],
            ];
            // set range key if defined
            if(isset($rangeKeyName))
            {
                $rangeKeyType = array_key_exists('S', $item[$rangeKeyName]) ? 'S' : 'N';
                $key[$rangeKeyName] = [$rangeKeyType => $item[$rangeKeyName][$rangeKeyType]];
            }
            $this->client->deleteItem([
                'TableName' => $table,
                'Key'       => $key
            ]);
        }
    }
}