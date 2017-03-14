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
    use FetchModeTrait { setFetchMode as protected; }

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
    private $rowCallback;

    public function __construct($table, $where, Db $db, array $options = []) {
        $this->table = $table;
        $this->where = $where;
        $this->db = $db;

        $options += [
            'fetchMode' => null,
            'rowCallback' => null
        ];

        $this->setFetchMode(...(array)$options['fetchMode']);
        $this->rowCallback = $options['rowCallback'];
    }

    /**
     * Retrieve an external iterator
     * @link http://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return Traversable Returns a generator of all rows.
     */
    public function getIterator() {
        foreach ($this->query() as $i => $row) {
            if ($this->rowCallback !== null) {
                $row = call_user_func($this->rowCallback, $row);
            }

            yield $i => $row;
        }
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
        $this->setOption('order', $columns);
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
        $this->setOption('offset', $offset);
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
     * Set the limit.
     *
     * @param int $limit
     * @return $this
     */
    public function setLimit($limit) {
        $this->setOption('limit', $limit);
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
     * @param $name
     * @param $value
     * @return $this
     */
    protected function setOption($name, $value) {
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
    function jsonSerialize() {
        return $this->getData();
    }

    /**
     * Perform the actual query to fetch the data.
     * @return mixed
     */
    private function query() {
        $result = $this->db->get($this->table, $this->where, $this->options);

        return $result;
    }
}
