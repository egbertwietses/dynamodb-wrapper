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
     * @param $keyconditions
     * @return \Aws\Common\Iterator\AwsResourceIterator AwsResourceIterator
     */
    public function getItem($tableName,$keyconditions,$index=null){
        
        try
        {
            $query = [
                'TableName'     => $tableName,
                'KeyConditions' => $keyconditions
            ];
            if($index){
                $query['IndexName'] = $index;
            }
            $iterator = $this->client->getIterator('Query',$query);
        }
        catch(\Exception $e){
            throw $e;
        }
        
        // Each item will contain the attributes we added
        foreach ($iterator as $dbitem) {
            $item = $this->extractMap($dbitem);
            return $item;
        }
        
        return false;
    }
    
    public function batchGetItem($tableName,$key,$ids){
        $keys = [];
        foreach($ids as $id){
            switch(gettype($id)){
                case 'integer':
                case 'float':
                    $type = 'N';
                    break;
                case 'string':
                    $type = 'S';
                    break;
            }
                
            $keys[] = [
                $key => [
                    $type => $id
                ]
            ];
        }
        
        $result = $this->client->batchGetItem(array(
            'RequestItems' => array(
                $tableName => array(
                    'Keys'           => $keys,
                    'ConsistentRead' => true
                )
            )
        ));
        
        $response = $result->getPath("Responses/{$tableName}");
        $items = [];
        foreach ($response as $dbitem) {
            $items[] = $this->extractMap($dbitem);
        }
        return $items;
    }
    
    private function extractMap(array $map){
        $obj = new \stdClass();
        foreach($map as $key => $value){
            $obj->$key = $this->extractValue($value);
        }
        return $obj;
    }
    
    private function extractValue(array $valuedef){
        $value = reset($valuedef);
        $type = key($valuedef);
        switch($type){
            case 'N':
                return (float) $value;
            case 'S':
                return $value;
            case 'NULL':
                return null;
            case 'BOOL':
                return $value=='true';
            case 'M':
                return $this->extractMap($value);
            case 'SS':
                $stringset = [];
                foreach($value as $key => $listvalue){
                    $stringset[$key] = $listvalue;
                }
                return $stringset;
            case 'NS':
                $numberset = [];
                foreach($value as $key => $listvalue){
                    $numberset[$key] = (float) $value;
                }
                return $numberset;
            case 'L':
                $list = [];
                foreach($value as $key => $listvalue){
                    $list[$key] = $this->extractValue($listvalue);
                }
                return $list;
        }
    }
    
    /**
     * @param $tableName
     * @param DynamoDbItem $item
     * @throws \Exception
     */
    public function putItem($tableName, DynamoDbItem $item)
    {
        try
        {
            $response = $this->client->putItem([
                "TableName"              => $tableName,
                "Item"                   => $item->getPutItemArray(),
                "ReturnConsumedCapacity" => "TOTAL"
            ]);
        }
        catch (\Exception $ex)
        {
            throw $ex;
        }
    }
    
    /**
     * 
     * @param string $tableName
     * @param array $keys key.value paired primarykeys
     * @return \Guzzle\Service\Resource\Model
     * @throws \Exception
     */
    public function deleteItem($tableName,$keys){
        try
        {
            $awskey = [];
            foreach($keys as $key => $value){
                switch(gettype($value)){
                    case 'integer':
                    case 'float':
                        $type = 'N';
                        break;
                    case 'string':
                        $type = 'S';
                        break;
                }
                $awskey[$key] = [
                    $type => $value
                ];
            }
            
            return $this->client->deleteItem([
                'TableName' => $tableName,
                'Key' => $awskey
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