<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

class ArraySet implements  \IteratorAggregate, DatasetInterface {
    use Utils\DatasetTrait;

    /**
     * @var array The internal data array.
     */
    private $data;

    /**
     * @var bool Indicates that the data needs to be sorted.
     */
    private $toSort = false;

    /**
     * Construct a new {@link ArraySet} object.
     *
     * @param array|\Traversable $data The initial data in the array.
     * @param string[] $order The default sort order. Supply this to prevent a manual sort.
     */
    public function __construct($data = [], $order = []) {
        $this->setData($data);
        $this->order = $order;
    }

    /**
     * Sort the internal array.
     */
    protected function sortData() {
        $columns = $this->getOrder();
        $order = [];
        foreach ($columns as $column) {
            if ($column[0] === '-') {
                $column = substr($column, 1);
                $order[$column] = -1;
            } else {
                $order[$column] = 1;
            }
        }

        $cmp = function ($a, $b) use ($order) {
            foreach ($order as $column => $desc) {
                $r = strnatcmp($a[$column], $b[$column]);

                if ($r !== 0) {
                    return $r * $desc;
                }
            }
            return 0;
        };

        usort($this->data, $cmp);
    }

    /**
     * Get the underlying data array.
     *
     * @return array Returns the data.
     */
    public function getData() {
        if ($this->toSort && !empty($this->getOrder())) {
            $this->sortData();
        }

        if ($this->getLimit()) {
            return array_slice($this->data, $this->getOffset(), $this->getLimit());
        } else {
            return $this->data;
        }
    }

    /**
     * Set the underlying data array.
     *
     * @param array|\Traversable $data
     * @return $this
     */
    public function setData($data) {
        if ($data instanceof \Traversable) {
            $this->data = iterator_to_array($data);
        } else {
            $this->data = $data;
        }

        if (!empty($this->getOrder())) {
            $this->toSort = true;
        }

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function setOrder(string ...$columns) {
        if ($columns !== $this->order) {
            $this->toSort = true;
        }

        $this->order = $columns;
        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function count(): int {
        $count = count($this->data);
        $limit = $this->getLimit();
        $offset = $this->getOffset();

        $count = $count - $offset;
        if ($limit > 0) {
            $count = min($count, $limit);
        }
        $count = max($count, 0);

        return $count;
    }
}
