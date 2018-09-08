<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

/**
 * Represents an aggregate function call.
 *
 * Use an aggregate by passing an instance as a value in the "columns" option on a call to {@link Db::get()}.
 */
class Aggregate extends Literal {
    const AVG = 'avg';
    const COUNT = 'count';
    const COUNT_DISTINCT = 'count-distinct';
    const MAX = 'max';
    const MIN = 'min';
    const SUM = 'sum';

    private $func;
    private $column;
    private $alias;

    /**
     * Construct an {@link Aggregate} object.
     *
     * @param string $func The name of the aggregate function. Use one of the **Aggregate::*** constants.
     * @param string $column The name of the column to aggregate.
     * @param string $alias The alias of the aggregate. If left out then the function name will be used as the alias.
     */
    public function __construct($func, $column, $alias = '') {
        $format = '%1$s(%2$s) as %3$s';
        if ($func === static::COUNT_DISTINCT) {
            $format = '%1$s(distinct %2$s) as %3$s';
            $func = static::COUNT;
        }

        parent::__construct($format);

        $this->func = $func;
        $this->column = $column;
        $this->alias = $alias;
    }

    /**
     * {@inheritdoc}
     */
    public function getValue(Db $db, ...$args) {
        return parent::getValue(
            $db,
            $this->func,
            $this->column === '*' ? '*' : $db->escape($this->column),
            $db->escape($this->alias ?: $this->func)
        );
    }
}
