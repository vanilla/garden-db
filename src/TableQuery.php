<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;
use Traversable;

class TableQuery implements \IteratorAggregate, DatasetInterface {
    use Utils\FetchModeTrait { setFetchMode as protected; }

    /**
     * @var Db
     */
    private $db;

    /**
     * @var array
     */
    private $data;

    /**
     * @var string
     */
    private $table;

    /**
     * @var
     */
    private $where;

    /**
     * @var array
     */
    private $options;

    /**
     * @var callable A callback used to unserialize rows from the database.
     */
    private $rowCallback;

    /**
     * Construct a new {@link TableQuery} object.
     *
     * Note that this class is not meant to be modified after being constructed so it's important to pass everything you
     * need early.
     *
     * @param string $table The name of the table to query.
     * @param array $where The filter information for the query.
     * @param Db $db The database to fetch the data from. This can be changed later.
     * @param array $options Additional options for the object:
     *
     * fetchMode
     * : The PDO fetch mode. This can be one of the **PDO::FETCH_** constants, a class name or an array of arguments for
     * {@link PDOStatement::fetchAll()}
     * rowCallback
     * : A callable that will be applied to each row of the result set.
     */
    public function __construct($table, array $where, Db $db, array $options = []) {
        $this->table = $table;
        $this->where = $where;
        $this->db = $db;

        $options += [
            Db::OPTION_FETCH_MODE => $db->getFetchArgs(),
            'rowCallback' => null
        ];

        $fetchMode = (array)$options[Db::OPTION_FETCH_MODE];
        if (in_array($fetchMode[0], [PDO::FETCH_OBJ | PDO::FETCH_NUM | PDO::FETCH_FUNC])) {
            throw new \InvalidArgumentException("Fetch mode not supported.", 500);
        } elseif ($fetchMode[0] == PDO::FETCH_CLASS && !is_a($fetchMode[1], \ArrayAccess::class, true)) {
            throw new \InvalidArgumentException("The {$fetchMode[1]} class must implement ArrayAccess.", 500);
        }

        $this->setFetchMode(...$fetchMode);
        $this->rowCallback = $options['rowCallback'];
    }

    /**
     * Retrieve an external iterator
     * @link http://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return Traversable Returns a generator of all rows.
     */
    public function getIterator() {
        return new \ArrayIterator($this->getData());
    }

    /**
     * Get the order.
     *
     * @return array Returns the order.
     */
    public function getOrder() {
        return $this->getOption('order', []);
    }

    /**
     * Set the order.
     *
     * @param string[] $columns The column names to order by.
     * @return $this
     */
    public function setOrder(...$columns) {
        $this->setOption('order', $columns, true);
        return $this;
    }

    /**
     * Get the offset.
     *
     * @return int Returns the offset.
     */
    public function getOffset() {
        return $this->getOption('offset', 0);
    }

    /**
     * Set the offset.
     *
     * @param int $offset
     * @return $this
     */
    public function setOffset($offset) {
        $this->setOption('offset', (int)$offset, true);
        return $this;
    }

    /**
     * Get the limit.
     *
     * @return int Returns the limit.
     */
    public function getLimit() {
        return $this->getOption('limit', Model::DEFAULT_LIMIT);
    }

    /**
     * {@inheritdoc}
     */
    public function setLimit($limit) {
        $reset = true;

        if (is_array($this->data) && $limit < $this->getLimit()) {
            $this->data = array_slice($this->data, 0, $limit);
            $reset = false;
        }

        $this->setOption('limit', (int)$limit, $reset);

        return $this;
    }

    public function getPage() {
        $result = intdiv($this->getOffset(), (int)$this->getLimit()) + 1;
        return $result;
    }

    public function setPage($page) {
        $this->setOffset(($page - 1) * $this->getLimit());
        return $this;
    }

    protected function getOption($name, $default = null) {
        return isset($this->options[$name]) ? $this->options[$name] : $default;
    }

    /**
     * Set a query option.
     *
     * @param string $name The name of the option to set.
     * @param mixed $value The new value of the option.
     * @param bool $reset Pass **true** and the data will be queried again if the option value has changed.
     * @return $this
     */
    protected function setOption($name, $value, $reset = false) {
        $changed = !isset($this->options[$name]) || $this->options[$name] !== $value;

        if ($changed && $reset) {
            $this->data = null;
        }

        $this->options[$name] = $value;
        return $this;
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
     * Get the data.
     *
     * @return array Returns the data.
     */
    public function getData() {
        if ($this->data === null) {
            $stmt = $this->db->get(
                $this->table,
                $this->where,
                $this->options + [Db::OPTION_FETCH_MODE => $this->getFetchArgs()]
            );

            $data = $stmt->fetchAll(...(array)$this->getFetchArgs());

            if (is_callable($this->rowCallback)) {
                array_walk($data, function (&$row) {
                    $row = call_user_func($this->rowCallback, $row);
                });
            }

            $this->data = $data;
        }

        return $this->data;
    }

    public function fetchAll($mode = 0, ...$args) {
        $thisMode = $this->getFetchMode();
        if ($mode === 0 || $mode === $thisMode || $this->data === []) {
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
            default:
                // Don't know what to do, fetch from the database again.
                $result = $this->db->get($this->table, $this->where, $this->options)->fetchAll($mode, ...$args);
        }
        return $result;
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
     * {@inheritdoc}
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
     * Get the number of records queried.
     *
     * @return int Returns the count.
     */
    public function count() {
        return count($this->getData());
    }
}
