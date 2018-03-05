<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;
use Traversable;

/**
 * Represents a dataset on a delayed query.
 *
 * This class is analogous to the {@link PDOStatement} except the query is only executed when the data is first accessed.
 */
class TableQuery implements \IteratorAggregate, DatasetInterface {
    use Utils\FetchModeTrait, Utils\DatasetTrait {
        fetchAll as protected fetchAllTrait;
        setFetchMode as protected;
    }

    /**
     * @var Db
     */
    private $db;

    /**
     * @var array|null
     */
    private $data;

    /**
     * @var string
     */
    private $table;

    /**
     * @var array
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
     * {@inheritdoc}
     */
    public function getOrder() {
        return $this->getOption('order', []);
    }

    /**
     * {@inheritdoc}
     */
    public function setOrder(string ...$columns) {
        $this->setOption('order', $columns, true);
        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function getOffset() {
        return $this->getOption('offset', 0);
    }

    /**
     * {@inheritdoc}
     */
    public function setOffset($offset) {
        if (!is_numeric($offset) || $offset < 0) {
            throw new \InvalidArgumentException("Invalid offset '$offset.'", 500);
        }

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
        if (!is_numeric($limit) || $limit < 0) {
            throw new \InvalidArgumentException("Invalid limit '$limit.'", 500);
        }


        $reset = true;

        if (is_array($this->data) && $limit < $this->getLimit()) {
            $this->data = array_slice($this->data, 0, $limit);
            $reset = false;
        }

        $this->setOption('limit', (int)$limit, $reset);

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
     * Get the data.
     *
     * @return array Returns the data.
     */
    protected function getData() {
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

    public function fetchAll(int $mode = 0, ...$args): array {
        $thisMode = $this->getFetchMode();
        if ($mode === 0 || $mode === $thisMode || $this->data === []) {
            return $this->getData();
        }

        $result = $this->fetchAllTrait($mode, ...$args);

        if ($result === null) {
            // Don't know what to do, fetch from the database again.
            $result = $this->db->get($this->table, $this->where, $this->options)->fetchAll($mode, ...$args);
        }
        return $result;
    }
}
