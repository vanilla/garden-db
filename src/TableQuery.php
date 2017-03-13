<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;
use Traversable;

class TableQuery implements DatasetInterface, \IteratorAggregate {
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
     * @var callable
     */
    private $calculator;

    public function __construct($table, $where, Db $db) {
        $this->table = $table;
        $this->where = $where;
        $this->db = $db;
    }

    /**
     * Retrieve an external iterator
     * @link http://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return Traversable An instance of an object implementing <b>Iterator</b> or
     * <b>Traversable</b>
     * @since 5.0.0
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
        $this->options['order'] = (array)$columns;
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
        $this->options['offset'] = $offset;
        return $this;
    }

    /**
     * Get the limit.
     *
     * @return int Returns the limit.
     */
    public function getLimit() {
        return $this->getOption('limit', 10);
    }

    /**
     * Set the limit.
     *
     * @param int $limit
     * @return $this
     */
    public function setLimit($limit) {
        $this->options['limit'] = $limit;
        return $this;
    }

    /**
     * Get the data.
     *
     * @return array Returns the data.
     */
    public function getData() {
        if ($this->data === null) {
            $this->data = $this->query();
        }

        return $this->data;
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
     * Specify data which should be serialized to JSON
     * @link http://php.net/manual/en/jsonserializable.jsonserialize.php
     * @return mixed data which can be serialized by <b>json_encode</b>,
     * which is a value of any type other than a resource.
     * @since 5.4.0
     */
    function jsonSerialize() {
        return $this->getData();
    }

    /**
     * Perform the actual query to fetch the data.
     * @return mixed
     */
    private function query() {
        $result = $this->db->get($this->table, $this->where, $this->options)->fetchAll(PDO::FETCH_ASSOC);
        if (isset($this->calculator)) {
            array_walk($result, $this->calculator);
        }

        return $result;
    }

    /**
     * Get the calculator.
     *
     * @return callable Returns the calculator.
     */
    public function getCalculator() {
        return $this->calculator;
    }

    /**
     * Set the calculator.
     *
     * @param callable|null $calculator
     * @return $this
     */
    public function setCalculator(callable $calculator) {
        $this->calculator = $calculator;
        return $this;
    }
}
