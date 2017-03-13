<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

/**
 * An object representation of a SELECT statement.
 */
class Query {
    /**
     * @var string
     */
    private $from;

    /**
     * @var array The where clause.
     */
    private $where;

    /**
     * @var array A pointer to the current where clause.
     */
    private $currentWhere;

    /**
     * @var array Pointers to nested where brackets.
     */
    private $whereStack;

    /**
     * @var int
     */
    private $limit;

    /**
     * @var int
     */
    private $offset;

    /**
     * Instantiate a new instance of the {@link Query} class.
     *
     * @param string $from The table to query.
     */
    public function __construct($from = '') {
        $this->from = $from;
        $this->where = [];
        $this->currentWhere = &$this->where;
        $this->whereStack = [];
    }

    /**
     * Begin an AND bracket group in the WHERE clause.
     *
     * @return $this
     * @see Query::end()
     */
    public function beginAnd() {
        $this->beginBracket(Db::OP_AND);
        return $this;
    }

    /**
     * Begin an OR bracket group in the WHERE clause.
     *
     * @return $this
     * @see Query::end()
     */
    public function beginOr() {
        $this->beginBracket(Db::OP_OR);
        return $this;
    }

    /**
     * Begin a bracket group in the WHERE clause.
     *
     * @param string $op One of the **DB::OP_*** constants.
     * @return $this
     */
    private function beginBracket($op) {
        $this->currentWhere[] = [$op => []];
        $this->whereStack[] = &$this->currentWhere;
        end($this->currentWhere);
        $this->currentWhere = &$this->currentWhere[key($this->currentWhere)][$op];

        return $this;
    }

    /**
     * End a bracket group.
     *
     * @return $this
     * @see Query::beginAnd(), Query::beginOr()
     */
    public function end() {
        // First unset the reference so it doesn't overwrite the original where clause.
        unset($this->currentWhere);

        // Move the pointer in the where stack back one level.
        if (empty($this->whereStack)) {
            trigger_error("Call to Query->end() without a corresponding call to Query->begin*().", E_USER_NOTICE);
            $this->currentWhere = &$this->where;
        } else {
            $key = key(end($this->whereStack));
            $this->currentWhere = &$this->whereStack[$key];
            unset($this->whereStack[$key]);

        }
        return $this;
    }

    /**
     * Add a WHERE condition to the query.
     *
     * A basic call to this method specifies a column name and a value.
     *
     * ```php
     * $query->where('id', 123);
     * ```
     *
     * If you specify just a value then the query will perform an equality match. If you want to specify a different
     * operator then you can pass in an array for {@link $value} with the key being the operator and the value being the
     * value to filter on.
     *
     * ```php
     * $query->where('count', [Db::GT => 100]);
     * ```
     *
     * @param string $column Either the name of the column or an array representing several query operators.
     * @param mixed $value The value to match to the column. If using the array form of {@link $column} then this parameter
     * can be omitted.
     * @return $this
     */
    public function addWhere($column, $value) {
        if (array_key_exists($column, $this->currentWhere)) {
            if (!is_array($this->currentWhere[$column])) {
                // This is a basic equality statement.
                $this->currentWhere[$column] = [Db::OP_EQ => $this->currentWhere[$column]];
            } elseif (array_key_exists(0, $this->currentWhere[$column])) {
                // This is a basic IN clause.
                $this->currentWhere[$column] = [Db::OP_IN => $this->currentWhere[$column]];
            }

            // Massage the value for proper syntax.
            if (!is_array($value)) {
                $value = [Db::OP_EQ => $value];
            } elseif (array_key_exists(0, $value)) {
                $value = [Db::OP_IN => $value];
            }

            $this->currentWhere[$column] = array_merge($this->currentWhere[$column], $value);
        } else {
            $this->currentWhere[$column] = $value;
        }
        return $this;
    }

    /**
     * Add a like statement to the current where clause.
     *
     * @param string $column The name of the column to compare.
     * @param string $value The like query.
     * @return $this
     */
    public function addLike($column, $value) {
        $r = $this->addWhere($column, [Db::OP_LIKE => $value]);
        return $r;
    }

    /**
     * Add an in statement to the current where clause.
     *
     * @param string $column The name of the column to compare.
     * @param array $values The in list to check against.
     * @return $this
     */
    public function addIn($column, array $values) {
        $r = $this->addWhere($column, [Db::OP_IN, $values]);
        return $r;
    }

    /**
     * Add an array of where statements.
     *
     * This method takes an array where the keys represent column names and the values represent filter values.
     *
     * ```php
     * $query->where([
     *     'parentID' => 123,
     *     'count' => [Db::GT => 0]
     * ]);
     * ```
     *
     * @param array $where The array of where statements to add to the query.
     * @return $this
     * @see Query::addWhere()
     */
    public function addWheres(array $where) {
        foreach ($where as $column => $value) {
            $this->addWhere($column, $value);
        }
        return $this;
    }

    /**
     * Return an array representation of this query.
     *
     * @return array Returns an array with keys representing the query parts.
     */
    public function toArray() {
        $r = [
            'from' => $this->from,
            'where' => $this->where,
            'limit' => $this->limit,
            'offset' => $this->offset
        ];

        return $r;
    }

    /**
     * Execute this query against a database.
     *
     * @param Db $db The database to query.
     * @return \PDOStatement Returns the query result.
     */
    public function exec(Db $db) {
        $options = [
            'limit' => $this->limit,
            'offset' => $this->offset
        ];

        $r = $db->get($this->from, $this->where, $options);
        return $r;
    }

    /**
     * Get the from.
     *
     * @return string Returns the from.
     */
    public function getFrom() {
        return $this->from;
    }

    /**
     * Set the from.
     *
     * @param string $from
     * @return $this
     */
    public function setFrom($from) {
        $this->from = $from;
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
        $this->limit = $limit;
        return $this;
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
        $this->offset = $offset;
        return $this;
    }
}
