<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

/**
 * A helper class for creating database tables.
 */
class TableDef implements \JsonSerializable {
    /// Properties ///

    /**
     * @var array The columns that need to be set in the table.
     */
    private $columns;

    /**
     *
     * @var string The name of the currently working table.
     */
    private $table;

    /**
     * @var array An array of indexes.
     */
    private $indexes;

    /// Methods ///

    /**
     * Initialize an instance of the {@link TableDef} class.
     *
     * @param string $name The name of the table.
     */
    public function __construct($name = '') {
        $this->reset();
        $this->table = $name;
    }

    /**
     * Reset the internal state of this object so that it can be re-used.
     *
     * @return TableDef Returns $this for fluent calls.
     */
    public function reset() {
        $this->table = '';
        $this->columns = [];
        $this->indexes = [];
        $this->options = [];

        return $this;
    }

    /**
     * Define a column.
     *
     * @param string $name The column name.
     * @param string $type The column type.
     * @param mixed $nullDefault Whether the column is required or it's default.
     *
     * null|true
     * : The column is not required.
     * false
     * : The column is required.
     * Anything else
     * : The column is required and this is its default.
     * @return TableDef
     */
    public function column($name, $type, $nullDefault = false) {
        $this->columns[$name] = $this->columnDef($type, $nullDefault);

        return $this;
    }

    /**
     * Get an array column def from a structured function call.
     *
     * @param string $type The database type of the column.
     * @param mixed $nullDefault Whether or not to allow null or the default value.
     *
     * null|true
     * : The column is not required.
     * false
     * : The column is required.
     * Anything else
     * : The column is required and this is its default.
     *
     * @return array Returns the column def as an array.
     */
    protected function columnDef($type, $nullDefault = false) {
        $column = ['type' => $type];

        if ($nullDefault === null || $nullDefault == true) {
            $column['required'] = false;
        }
        if ($nullDefault === false) {
            $column['required'] = true;
        } else {
            $column['required'] = true;
            $column['default'] = $nullDefault;
        }

        return $column;
    }

    /**
     * Define the primary key in the database.
     *
     * @param string $name The name of the column.
     * @param string $type The datatype for the column.
     * @return TableDef
     */
    public function primaryKey($name, $type = 'int') {
        $column = $this->columnDef($type, false);
        $column['autoincrement'] = true;
        $column['primary'] = true;

        $this->columns[$name] = $column;

        // Add the pk index.
        $this->index($name, Db::INDEX_PK);

        return $this;
    }

    /**
     * Set the name of the table.
     *
     * @param string|null $name The name of the table.
     * @return TableDef|string Returns $this for fluent calls.
     */
    public function table($name = null) {
        if ($name !== null) {
            $this->table = $name;
            return $this;
        }
        return $this->table;
    }

    /**
     * Add or update an index.
     *
     * @param string|array $columns An array of columns or a single column name.
     * @param string $type One of the `Db::INDEX_*` constants.
     * @param string $suffix An index suffix to group columns together in an index.
     * @return TableDef Returns $this for fluent calls.
     */
    public function index($columns, $type, $suffix = '') {
        $type = strtolower($type);
        $columns = (array)$columns;
        $suffix = strtolower($suffix);

        // Look for a current index row.
        $currentIndex = null;
        foreach ($this->indexes as $i => $index) {
            if ($type !== $index['type']) {
                continue;
            }

            $indexSuffix = empty($index['suffix']) ? '' : $index['suffix'];

            if ($type === Db::INDEX_PK ||
                ($type === Db::INDEX_UNIQUE && $suffix == $indexSuffix) ||
                ($type === Db::INDEX_IX && $suffix && $suffix == $indexSuffix) ||
                ($type === Db::INDEX_IX && !$suffix && array_diff($index['columns'], $columns) == [])
            ) {
                $currentIndex =& $this->indexes[$i];
                break;
            }
        }

        if ($currentIndex) {
            $currentIndex['columns'] = array_unique(array_merge($currentIndex['columns'], $columns));
        } else {
            $indexDef = [
                'type' => $type,
                'columns' => $columns,
                'suffix' => $suffix
            ];
            if ($type === Db::INDEX_PK) {
                $this->indexes[Db::INDEX_PK] = $indexDef;
            } else {
                $this->indexes[] = $indexDef;
            }
        }

        return $this;
    }

    /**
     * Specify data which should be serialized to JSON.
     *
     * @link http://php.net/manual/en/jsonserializable.jsonserialize.php
     * @return mixed data which can be serialized by {@link json_encode()},
     * which is a value of any type other than a resource.
     */
    public function jsonSerialize() {
        return $this->toArray();
    }

    /**
     * Get the array representation of the table definition.
     *
     * @return array Returns a definition array.
     */
    public function toArray() {
        return [
            'name' => $this->table,
            'columns' => $this->columns,
            'indexes' => $this->indexes
        ];
    }
}
