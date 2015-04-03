<?php namespace EgbertWietses\DynamoDbWrapper;

/**
 * Class DynamoDbItem
 */
class DynamoDbItem {

    /**
     * @var array
     */
    private $values;

    /**
     * @var array
     */
    private $keys = [];

    /**
     * @param array $keys
     */
    public function __construct(array $keys = [])
    {
        if( ! empty($keys))
        {
            $this->keys = $keys;
        }
    }

    /**
     * Pass an array of key names to tell the item what the keys are. Used by deleteItem for example.
     *
     * @param array $keys
     */
    public function setKeys(array $keys)
    {
        $this->keys = $keys;
    }

    /**
     * @param $key
     * @param $value
     */
    public function addValue($key, $value)
    {
        $this->values[$key] = $value;
    }

    /**
     * @param array $array
     */
    public function importArray(array $array)
    {
        $this->values = $array;
    }

    /**
     * @return array
     */
    public function getPutItemArray()
    {
        return $this->convertToDynamoArray($this->values);
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function getDeleteItemArray()
    {
        $deleteArray = [];

        if(empty($this->keys))
        {
            throw new \Exception('Keys must be set when trying to delete an item.');
        }

        foreach($this->keys as $key)
        {
            $deleteArray[$key] = $this->values[$key];
        }

        return $this->convertToDynamoArray($deleteArray);
    }

    /**
     * @param array $array
     * @return bool
     */
    private function isMap(array $array)
    {
        return (bool) count(array_filter(array_keys($array), 'is_string'));
    }

    /**
     * @param array $values
     * @return array
     */
    private function convertToDynamoArray(array $values)
    {
        $dynamoValues = [];

        foreach ($values as $key => $value)
        {
            switch (gettype($value))
            {
                case 'array':
                    if($this->isMap($value))
                    {
                        $type = 'M';
                    }
                    else
                    {
                        $type = 'L';
                    }

                    $value = $this->convertToDynamoArray($value);

                    break;
                case 'object':
                    $type = 'M';

                    //Convert to array
                    $value = json_decode(json_encode($value), true);

                    $value = $this->convertToDynamoArray($value);

                    break;

                case 'string':
                    if(ctype_digit($value))
                    {
                        $type = 'N';
                        break;
                    }

                    if($value === '')
                    {
                        $type = 'NULL';
                        $value = true;

                        break;
                    }

                    $type = 'S';

                    break;
                case 'integer':
                case 'double':
                    $type = 'N';
                    break;
                case 'boolean':
                    $type = 'BOOL';
                    break;
                case 'NULL':
                    $type = 'NULL';
                    $value = true;
                    break;
                default:
                    $type = false;
            }

            //We don't want to add strange values to Dynamo with danger of getting an exception.
            if($type === false || is_null($type) || $key === '')
            {
                continue;
            }

            $dynamoValues[$key] = [
                $type => $value
            ];

        }
        return $dynamoValues;
    }
}