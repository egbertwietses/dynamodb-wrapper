<?php namespace EgbertWietses\DynamoDbWrapper;

/**
 * Class DynamoDbItem
 */
class DynamoDbItem {

    public $values;

    public function __construct()
    {

    }

    public function addValue($key, $value)
    {
        $this->values[$key] = $value;
    }

    public function importArray(array $array)
    {
        $this->values = $array;
    }

    public function getPutItemArray()
    {
        return $this->convertToDynamoArray($this->values);
    }

    private function isMap(array $array)
    {
        return (bool) count(array_filter(array_keys($array), 'is_string'));
    }

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
            if($type === false || is_null($type))
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