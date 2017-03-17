<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;

trait DatasetTrait {
    /**
     * @var int
     */
    private $offset = 0;

    /**
     * @var int
     */
    private $limit = 0;

    /**
     * @var string[]
     */
    private $order;

    /**
     * Get the dataset array.
     *
     * @return array
     */
    abstract public function getData();

    public function getPage() {
        if ($this->getLimit() === 0) {
            return 1;
        }
        $result = floor($this->getOffset() / (int)$this->getLimit()) + 1;
//        $result = intdiv($this->getOffset(), (int)$this->getLimit()) + 1;
        return (int)$result;
    }

    public function setPage($page) {
        if (!is_numeric($page) || $page < 0) {
            throw new \InvalidArgumentException("Invalid page '$page.'", 500);
        }

        $this->setOffset(($page - 1) * $this->getLimit());
        return $this;
    }

    /**
     * Retrieve an external iterator
     * @link http://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return Traversable Returns a generator of all rows.
     */
    public function getIterator() {
        return new \ArrayIterator($this->getData());
    }

    public function fetchAll($mode = 0, ...$args) {
        if ($mode === 0) {
            return $this->getData();
        }

        switch ($mode) {
            case PDO::FETCH_COLUMN:
                $result = $this->fetchArrayColumn(reset($args) ?: 0);
                break;
            case PDO::FETCH_COLUMN | PDO::FETCH_GROUP;
                $result = $this->fetchArrayColumn(0, reset($args) ?: 0, true);
                break;
            case PDO::FETCH_KEY_PAIR:
                $result = $this->fetchArrayColumn(1, 0);
                break;
            case PDO::FETCH_UNIQUE:
                $result = $this->fetchArrayColumn(null, reset($args) ?: 0);
                break;
            case PDO::FETCH_OBJ:
                $result = array_map(function ($row) {
                    return (object)$row;
                }, $this->getData());
                break;
            case PDO::FETCH_ASSOC:
                $result = array_map(function ($row) {
                    return (array)$row;
                }, $this->getData());
                break;
            default:
                // Don't know what to do, return null.
                $result = null;
        }
        return $result;
    }

    /**
     * Fetch the data and perform a quasi-{@link array_column()} operation on it.
     *
     * @param string|int|null $columnKey The key or ordinal of the value or **null** to return the entire row.
     * @param string|int|null $indexKey The key or ordinal of the index or **null** to not index the data.
     * @param bool $grouped If true the result will be grouped by {@link $indexKey} and each value will be an array of rows.
     * @return array Returns the array of results.
     */
    public function fetchArrayColumn($columnKey = null, $indexKey = null, $grouped = false) {
        $arr = $this->getData();

        if (empty($arr)) {
            return [];
        }

        $firstRow = reset($arr);

        if (is_int($columnKey) || is_int($indexKey)) {
            $i = 0;
            foreach ($firstRow as $name => $value) {
                if ($i === $columnKey) {
                    $columnKey = $name;
                }

                if ($i === $indexKey) {
                    $indexKey = $name;
                }

                if (!(is_int($columnKey) || is_int($indexKey))) {
                    break;
                }
                $i++;
            }
        }

        if (!$grouped && is_array($firstRow)) {
            return array_column($arr, $columnKey, $indexKey);
        } else {
            $result = [];

            foreach ($arr as $i => $row) {
                if (is_array($row) || $row instanceof \ArrayAccess ) {
                    $value = $columnKey === null ? $row : $row[$columnKey];
                    $index = $indexKey === null ? $i : $row[$indexKey];
                } else {
                    $value = $columnKey === null ? $row : $row->$columnKey;
                    $index = $indexKey === null ? $i : $row->$indexKey;
                }

                if ($grouped) {
                    $result[$index][] = $value;
                } else {
                    $result[$index] = $value;
                }
            }

            return $result;
        }
    }

    /**
     * Get the first row of data.
     *
     * @return mixed|null Returns the first row or **null** if there is no data.
     */
    public function firstRow() {
        $data = $this->getData();

        return empty($data) ? null : $data[0];
    }

    /**
     * Get the number of records queried.
     *
     * @return int Returns the count.
     */
    public function count() {
        return count($this->getData());
    }

    /**
     * Specify data which should be serialized to JSON
     * @link http://php.net/manual/en/jsonserializable.jsonserialize.php
     * @return mixed data which can be serialized by <b>json_encode</b>,
     * which is a value of any type other than a resource.
     * @since 5.4.0
     */
    public function jsonSerialize() {
        return $this->getData();
    }

    /**
     * Get the offset.
     *
     * @return int Returns the offset.
     */
    public function getOffset() {
        return $this->offset;
    }

    /**
     * Set the offset.
     *
     * @param int $offset
     * @return $this
     */
    public function setOffset($offset) {
        if (!is_numeric($offset) || $offset < 0) {
            throw new \InvalidArgumentException("Invalid offset '$offset.'", 500);
        }
        
        $this->offset = (int)$offset;
        return $this;
    }

    /**
     * Get the limit.
     *
     * @return int Returns the limit.
     */
    public function getLimit() {
        return $this->limit;
    }

    /**
     * Set the limit.
     *
     * @param int $limit
     * @return $this
     */
    public function setLimit($limit) {
        if (!is_numeric($limit) || $limit < 0) {
            throw new \InvalidArgumentException("Invalid limit '$limit.'", 500);
        }

        $this->limit = (int)$limit;
        return $this;
    }

    /**
     * Get the sort order.
     *
     * @return string[] Returns an array of column names, optionally prefixed with "-" to denote descending order.
     */
    public function getOrder() {
        return $this->order;
    }

    /**
     * Set the sort order.
     *
     * @param string[] $columns The column names to sort by, optionally prefixed with "-" to denote descending order.
     * @return $this
     */
    public function setOrder(...$columns) {
        $this->order = $columns;
        return $this;
    }
}