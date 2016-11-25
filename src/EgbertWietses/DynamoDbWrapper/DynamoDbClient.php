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
            'region'      => $region,
            'credentials' => [
                'key'    => $key,
                'secret' => $secret
            ],
            'version'     => '2012-08-10'
        ]);
    }

    /**
     * @param $tableName
     * @param $keys
     * @return \stdClass
     */
    public function getItem($tableName, $keys)
    {
        $keys = $this->filterNonStringValues($keys);

        $result = $this->client->getItem([
            'ConsistentRead' => true,
            'TableName'      => $tableName,
            'Key'            => $keys
        ]);

        if ( ! isset($result['Item'])) {
            return false;
        }

        return $this->extractMap($result['Item']);
    }

    public function getItemByIndexValues($tableName, $indexName, array $keys)
    {
        $attributeValues = [];
        $attributeNames  = [];
        $expression      = '';

        foreach ($keys as $fieldName => $value) {
            if (ctype_digit((string) $value)) {
                $type = 'N';
            } else {
                $type = 'S';
            }

            if ( ! empty($expression)) {
                $expression .= ' and ';
            }

            $attributePlaceholder = '#n_' . addslashes($fieldName);
            $parameterPlaceholder = ':v_' . addslashes($fieldName);
            $expression .= $attributePlaceholder . ' = ' . $parameterPlaceholder;

            $attributeNames[$attributePlaceholder]  = addslashes($fieldName);
            $attributeValues[$parameterPlaceholder] = [$type => (string) $value];
        }

        $query    = [
            'TableName'                 => $tableName,
            'IndexName'                 => $indexName,
            'KeyConditionExpression'    => $expression,
            'ExpressionAttributeNames'  => $attributeNames,
            'ExpressionAttributeValues' => $attributeValues
        ];

        $iterator = $this->client->getIterator('Query',$query);

        foreach ($iterator as $item) {
            $item = $this->extractMap($item);
            return $item;
        }

        return false;
    }

    /**
     * @param      $tableName
     * @param      $keyconditions
     * @param null $index
     * @return bool|\stdClass
     * @throws \Exception
     */
    public function query($tableName,$keyconditions,$index=null){
        
        try
        {
            $keyconditions = $this->filterNonStringValues($keyconditions);
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
                case 'double':
                    $type = 'N';
                    break;
                case 'string':
                    if(ctype_digit($id)) {
                        $type = 'N';
                        break;
                    }
                    $type = 'S';
                    break;
            }
                
            $keys[] = [
                $key => [
                    $type => (string) $id
                ]
            ];
        }
        
        $result = $this->client->batchGetItem([
            'RequestItems' => [
                $tableName => [
                    'Keys'           => $keys,
                    'ConsistentRead' => true
                ]
            ]
        ]);
        
        $response = $result->getPath("Responses/{$tableName}");
        $items = collect();
        foreach ($response as $dbitem) {
            $items->push($this->extractMap($dbitem));
        }
        return $items;
    }
    
    private function extractMap(array $map, $numbersAsString = false){
        $obj = new \stdClass();
        foreach($map as $key => $value){
            $obj->$key = $this->extractValue($value, $numbersAsString);
        }
        return $obj;
    }
    
    private function extractValue(array $valuedef, $numbersAsString = false){
        $value = reset($valuedef);
        $type = key($valuedef);
        switch($type){
            case 'N':
                return $numbersAsString ? $value : (float) $value;
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
                    $numberset[$key] = $numbersAsString ? $value : (float) $value;
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
                "Item"                   => $this->filterNonStringValues($item->getPutItemArray()),
                "ReturnConsumedCapacity" => "TOTAL"
            ]);
        }
        catch (\Exception $ex)
        {
            throw $ex;
        }
    }
    
    /**
     * @param              $tableName
     * @param DynamoDbItem $item
     * @return \Aws\Result
     * @throws \Exception
     */
    public function deleteItem($tableName, DynamoDbItem $item){
        try
        {
            return $this->client->deleteItem([
                'TableName' => $tableName,
                'Key' => $item->getDeleteItemArray()
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
    
    public function scanTable($table) {
        $values = collect();
        $scan = $this->client->getIterator('Scan', ['TableName' => $table]);
        
        foreach ($scan as $value) {
            $values->push($this->extractMap($value));
        }
        
        return $values;
    }
    
    /**
     * @param $keyconditions
     * @return mixed
     */
    protected function filterNonStringValues($keyconditions)
    {
        array_walk_recursive($keyconditions, function (&$value) {
            if (is_numeric($value)) {
                $value = (string) $value;
            }
        });

        return $keyconditions;
    }
    
    /**
     * @return static
     */
    public function getClient()
    {
        return $this->client;
    }
}