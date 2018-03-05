<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

/**
 * Represents the functionality that a query result should implement.
 */
interface DatasetInterface extends \Traversable, \Countable, \JsonSerializable {
    /**
     * Get the number of records to skip in the query.
     *
     * @return int Returns the offset.
     */
    public function getOffset();

    /**
     * Set the record offset.
     *
     * Note that setting the offset will also affect the current page number.
     * @param int $offset The new offset.
     * @return $this
     */
    public function setOffset($offset);

    /**
     * Get the number of records to limit the query.
     *
     * @return int Returns the limit,
     */
    public function getLimit();

    /**
     * Set the number of records to limit the query.
     *
     * Note that setting the limit will also affect the current page number.
     * @param int $limit The new limit.
     * @return $this
     */
    public function setLimit($limit);

    /**
     * Get the current page number.
     *
     * @return int Returns the page number.
     */
    public function getPage(): int;

    /**
     * Set the current page.
     *
     * Note that setting the page will affect the current offset.
     *
     * @param int $page The new page number.
     * @return $this
     */
    public function setPage(int $page);

    /**
     * Get the sort order.
     *
     * @return string[] Returns an array of column names, optionally prefixed with "-" to denote descending order.
     */
    public function getOrder();

    /**
     * Set the sort order.
     *
     * @param string ...$columns The column names to sort by, optionally prefixed with "-" to denote descending order.
     * @return $this
     */
    public function setOrder(string ...$columns);

    /**
     * Fetch all data in a way similar to {@link PDOStatement::fetchAll()}.
     *
     * @param int $mode
     * @param mixed ...$args
     * @return array
     */
    public function fetchAll(int $mode = 0, ...$args): array;

    /**
     * Fetch the data and perform a quasi-{@link array_column()} operation on it.
     *
     * @param string|int|null $columnKey The key or ordinal of the value or **null** to return the entire row.
     * @param string|int|null $indexKey The key or ordinal of the index or **null** to not index the data.
     * @param bool $grouped If true the result will be grouped by {@link $indexKey} and each value will be an array of rows.
     * @return array Returns the array of results.
     */
    public function fetchArrayColumn($columnKey = null, $indexKey = null, bool $grouped = false): array;

    /**
     * Get the first row in the dataset.
     *
     * @return mixed
     */
    public function firstRow();
}
