<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

/**
 * Defines a standard set of methods that all database drivers must conform to.
 */
abstract class Db {
    /// Constants ///

    const QUERY_DEFINE = 'define';
    const QUERY_READ = 'read';
    const QUERY_WRITE = 'write';

    const INDEX_PK = 'primary';
    const INDEX_IX = 'index';
    const INDEX_UNIQUE = 'unique';

    const OPTION_REPLACE = 'replace';
    const OPTION_IGNORE = 'ignore';
    const OPTION_UPSERT = 'upsert';
    const OPTION_TRUNCATE = 'truncate';
    const OPTION_DROP = 'drop';
    const OPTION_MODE = 'mode';

    const OP_EQ = '=';
    const OP_GT = '>';
    const OP_GTE = '>=';
    const OP_IN = '$in';
    const OP_LIKE = '$like';
    const OP_LT = '<';
    const OP_LTE = '<=';
    const OP_NEQ = '<>';

    const OP_AND = '$and';
    const OP_OR = '$or';

    const ORDER_ASC = 'asc';
    const ORDER_DESC = 'desc';

    const FETCH_TABLENAMES = 0x1;
    const FETCH_COLUMNS = 0x2;
    const FETCH_INDEXES = 0x4;

    const MODE_EXEC = 0x1;
    const MODE_ECHO = 0x2;
    const MODE_SQL = 0x4;
    const MODE_PDO = 0x8;

    /// Properties ///

    /**
     * @var string The database prefix.
     */
    protected $px = 'gdn_';

    /**
     * @var int The query execution mode.
     */
    protected $mode = Db::MODE_EXEC;

    /**
     * @var array A cached copy of the table schemas.
     */
    protected $tables = [];

    /**
     * @var int Whether or not all the tables have been fetched.
     */
    protected $allTablesFetched = 0;

    /**
     * @var int The number of rows that were affected by the last query.
     */
    protected $rowCount;

    /// Methods ///

    /**
     * Add a table to the database.
     *
     * @param array $tableDef The table definition.
     * @param array $options An array of additional options when adding the table.
     */
    abstract protected function createTable(array $tableDef, array $options = []);

    /**
     * Alter a table in the database.
     *
     * When altering a table you pass an array with three optional keys: add, drop, and alter.
     * Each value is consists of a table definition in a format that would be passed to {@link Db::setTableDef()}.
     *
     * @param array $alterDef The alter definition.
     * @param array $options An array of additional options when adding the table.
     */
    abstract protected function alterTable(array $alterDef, array $options = []);

    /**
     * Drop a table.
     *
     * @param string $tableName The name of the table to drop.
     * @param array $options An array of additional options when adding the table.
     */
    abstract public function dropTable($tableName, array $options = []);

    /**
     * Get a table definition.
     *
     * @param string $tableName The name of the table.
     * @return array|null Returns the table definition or null if the table does not exist.
     */
    public function getTableDef($tableName) {
        $ltablename = strtolower($tableName);

        // Check to see if the table isn't in the cache first.
        if ($this->allTablesFetched & Db::FETCH_TABLENAMES &&
            !isset($this->tables[$ltablename])
        ) {
            return null;
        }

        if (
            isset($this->tables[$ltablename]) &&
            is_array($this->tables[$ltablename]) &&
            isset($this->tables[$ltablename]['columns'], $this->tables[$ltablename]['indexes'])
        ) {
            return $this->tables[$ltablename];
        }
        return [];
    }

    /**
     * Get all of the tables in the database.
     *
     * @param bool $withDefs Whether or not to return the full table definitions or just the table names.
     * @return array Returns an array of either the table definitions or the table names.
     */
    public function getAllTables($withDefs = false) {
        if ($withDefs && ($this->allTablesFetched & Db::FETCH_COLUMNS)) {
            return $this->tables;
        } elseif (!$withDefs && ($this->allTablesFetched & Db::FETCH_TABLENAMES)) {
            return array_keys($this->tables);
        } else {
            return null;
        }
    }

    /**
     * Set a table definition to the database.
     *
     * @param array $tableDef The table definition.
     * @param array $options An array of additional options when adding the table.
     */
    public function defineTable(array $tableDef, array $options = []) {
        $tableName = $tableDef['name'];
        $lTableName = strtolower($tableName);
        $tableDef['name'] = $tableName;
        $drop = self::val(Db::OPTION_DROP, $options, false);
        $curTable = $this->getTableDef($tableName);

        $this->fixIndexes($tableName, $tableDef, $curTable);

        if (!$curTable) {
            $this->createTable($tableDef, $options);
            $this->tables[$lTableName] = $tableDef;
            return;
        }
        // This is the alter statement.
        $alterDef = ['name' => $tableName];

        // Figure out the columns that have changed.
        $curColumns = (array)self::val('columns', $curTable, []);
        $newColumns = (array)self::val('columns', $tableDef, []);

        $alterDef['add']['columns'] = array_diff_key($newColumns, $curColumns);
        $alterDef['alter']['columns'] = array_uintersect_assoc($newColumns, $curColumns, function ($new, $curr) {
            // Return 0 if the values are different, not the same.
            if (self::val('dbtype', $curr) !== self::val('dbtype', $new) ||
                self::val('allowNull', $curr) !== self::val('allowNull', $new) ||
                self::val('default', $curr) !== self::val('default', $new)
            ) {
                return 0;
            }
            return 1;
        });

        // Figure out the indexes that have changed.
        $curIndexes = (array)self::val('indexes', $curTable, []);
        $newIndexes = (array)self::val('indexes', $tableDef, []);

        $alterDef['add']['indexes'] = array_udiff($newIndexes, $curIndexes, [$this, 'indexCompare']);

        $dropIndexes = array_udiff($curIndexes, $newIndexes, [$this, 'indexCompare']);
        if ($drop) {
            $alterDef['drop']['columns'] = array_diff_key($curColumns, $newColumns);
            $alterDef['drop']['indexes'] = $dropIndexes;
        } else {
            $alterDef['drop']['columns'] = [];
            $alterDef['drop']['indexes'] = [];

            // If the primary key has changed then the old one needs to be dropped.
            if (isset($dropIndexes[Db::INDEX_PK])) {
                $alterDef['drop']['indexes'][Db::INDEX_PK] = $dropIndexes[Db::INDEX_PK];
            }
        }

        // Check to see if any alterations at all need to be made.
        if (empty($alterDef['add']['columns']) && empty($alterDef['add']['indexes']) &&
            empty($alterDef['drop']['columns']) && empty($alterDef['drop']['indexes']) &&
            empty($alterDef['alter']['columns'])
        ) {
            return;
        }

        $alterDef['def'] = $tableDef;

        // Alter the table.
        $this->alterTable($alterDef, $options);

        // Update the cached schema.
        $tableDef['name'] = $tableName;
        $this->tables[$lTableName] = $tableDef;
    }

    /**
     * Move the primary key index into the correct place for database drivers.
     *
     * @param string $tableName The name of the table.
     * @param array &$tableDef The table definition.
     * @param array|null $curTableDef The current database table def used to resolve conflicts in some names.
     * @throws \Exception Throws an exception when there is a mismatch between the primary index and the primary key
     * defined on the columns themselves.
     */
    private function fixIndexes($tableName, array &$tableDef, $curTableDef = null) {
        // Loop through the columns and add get the primary key index.
        $primaryColumns = [];
        foreach ($tableDef['columns'] as $cname => $cdef) {
            if (self::val('primary', $cdef)) {
                $primaryColumns[] = $cname;
            }
        }

        // Massage the primary key index.
        $primaryFound = false;
        array_touch('indexes', $tableDef, []);
        foreach ($tableDef['indexes'] as &$indexDef) {
            array_touch('name', $indexDef, $this->buildIndexName($tableName, $indexDef));

            if (self::val('type', $indexDef) === Db::INDEX_PK) {
                $primaryFound = true;

                if (empty($primaryColumns)) {
                    foreach ($indexDef['columns'] as $cname) {
                        $tableDef['columns'][$cname]['primary'] = true;
                    }
                } elseif (array_diff($primaryColumns, $indexDef['columns'])) {
                    throw new \Exception("There is a mismatch in the primary key index and primary key columns.", 500);
                }
            } elseif (isset($curTableDef['indexes'])) {
                $curIndexDef = array_usearch($indexDef, $curTableDef['indexes'], [$this, 'indexCompare']);
                if ($curIndexDef && isset($curIndexDef['name'])) {
                    $indexDef['name'] = $curIndexDef['name'];
                }
            }
        }

        if (!$primaryFound && !empty($primaryColumns)) {
            $tableDef['indexes'][db::INDEX_PK] = [
                'columns' => $primaryColumns,
                'type' => Db::INDEX_PK
            ];
        }
    }

    /**
     * Get the database prefix.
     *
     * @return string Returns the current db prefix.
     */
    public function getPx() {
        return $this->px;
    }

    /**
     * Set the database prefix.
     *
     * @param string $px The new database prefix.
     */
    public function setPx($px) {
        $this->px = $px;
    }

    /**
     * Compare two index definitions to see if they have the same columns and same type.
     *
     * @param array $a The first index.
     * @param array $b The second index.
     * @return int Returns an integer less than, equal to, or greater than zero if {@link $a} is
     * considered to be respectively less than, equal to, or greater than {@link $b}.
     */
    public function indexCompare(array $a, array $b) {
        if ($a['columns'] > $b['columns']) {
            return 1;
        } elseif ($a['columns'] < $b['columns']) {
            return -1;
        }

        return strcmp(self::val('type', $a, ''), self::val('type', $b, ''));
    }

    /**
     * Get data from the database.
     *
     * @param string $tableName The name of the table to get the data from.
     * @param array $where An array of where conditions.
     * @param array $options An array of additional options.
     * @return mixed Returns the result set.
     */
    abstract public function get($tableName, array $where, array $options = []);

    /**
     * Get a single row from the database.
     *
     * This is a conveinience method that calls {@link Db::get()} and shifts off the first row.
     *
     * @param string $tableName The name of the table to get the data from.
     * @param array $where An array of where conditions.
     * @param array $options An array of additional options.
     * @return array|false Returns the row or false if there is no row.
     */
    public function getOne($tableName, array $where, array $options = []) {
        $options['limit'] = 1;
        $rows = $this->get($tableName, $where, $options);
        return array_shift($rows);
    }

    /**
     * Insert a row into a table.
     *
     * @param string $tableName The name of the table to insert into.
     * @param array $row The row of data to insert.
     * @param array $options An array of options for the insert.
     *
     * Db::OPTION_IGNORE
     * : Whether or not to ignore inserts that lead to a duplicate key. *default false*
     * Db::OPTION_REPLACE
     * : Whether or not to replace duplicate keys. *default false*
     * Db::OPTION_UPSERT
     * : Whether or not to update the existing data when duplicate keys exist.
     *
     * @return mixed Should return the id of the inserted record.
     * @see Db::load()
     */
    abstract public function insert($tableName, array $row, array $options = []);

    /**
     * Load many rows into a table.
     *
     * @param string $tableName The name of the table to insert into.
     * @param \Traversable|array $rows A dataset to insert.
     * Note that all rows must contain the same columns.
     * The first row will be looked at for the structure of the insert and the rest of the rows will use this structure.
     * @param array $options An array of options for the inserts. See {@link Db::insert()} for details.
     * @return mixed
     * @see Db::insert()
     */
    public function load($tableName, $rows, array $options = []) {
        foreach ($rows as $row) {
            $this->insert($tableName, $row, $options);
        }
    }


    /**
     * Update a row or rows in a table.
     *
     * @param string $tableName The name of the table to update.
     * @param array $set The values to set.
     * @param array $where The where filter for the update.
     * @param array $options An array of options for the update.
     * @return mixed
     */
    abstract public function update($tableName, array $set, array $where, array $options = []);

    /**
     * Delete rows from a table.
     *
     * @param string $tableName The name of the table to delete from.
     * @param array $where The where filter of the delete.
     * @param array $options An array of options.
     *
     * Db:OPTION_TRUNCATE
     * : Truncate the table instead of deleting rows. In this case {@link $where} must be blank.
     * @return mixed
     */
    abstract public function delete($tableName, array $where, array $options = []);

    /**
     * Reset the internal table definition cache.
     *
     * @return Db Returns $this for fluent calls.
     */
    public function reset() {
        $this->tables = [];
        $this->allTablesFetched = 0;
        $this->rowCount = 0;
        return $this;
    }

    /**
     * Build a standardized index name from an index definition.
     *
     * @param string $tableName The name of the table the index is in.
     * @param array $indexDef The index definition.
     * @return string Returns the index name.
     */
    protected function buildIndexName($tableName, array $indexDef) {
        $type = self::val('type', $indexDef, Db::INDEX_IX);

        if ($type === Db::INDEX_PK) {
            return 'primary';
        }
        $px = self::val($type, [Db::INDEX_IX => 'ix_', Db::INDEX_UNIQUE => 'ux_'], 'ix_');
        $sx = self::val('suffix', $indexDef);
        $result = $px.$tableName.'_'.($sx ?: implode('', $indexDef['columns']));
        return $result;
    }

    /**
     * Safely get a value out of an array.
     *
     * This function will always return a value even if the array key doesn't exist.
     * The self::val() function is one of the biggest workhorses of Vanilla and shows up a lot throughout other code.
     * It's much preferable to use this function if your not sure whether or not an array key exists rather than
     * using @ error suppression.
     *
     * This function uses optimizations found in the [facebook libphputil library](https://github.com/facebook/libphutil).
     *
     * @param string|int $key The array key.
     * @param array|object $array The array to get the value from.
     * @param mixed $default The default value to return if the key doesn't exist.
     * @return mixed The item from the array or `$default` if the array key doesn't exist.
     * @category Array Functions
     */
    protected static function val($key, $array, $default = null) {
        if (is_array($array)) {
            // isset() is a micro-optimization - it is fast but fails for null values.
            if (isset($array[$key])) {
                return $array[$key];
            }

            // Comparing $default is also a micro-optimization.
            if ($default === null || array_key_exists($key, $array)) {
                return null;
            }
        } elseif (is_object($array)) {
            if (isset($array->$key)) {
                return $array->$key;
            }

            if ($default === null || property_exists($array, $key)) {
                return null;
            }
        }

        return $default;
    }
}

/**
 * Strip a substring from the beginning of a string.
 *
 * @param string $mainstr The main string to look at (the haystack).
 * @param string $substr The substring to search trim (the needle).
 * @return string
 *
 * @category String Functions
 */
function ltrim_substr($mainstr, $substr) {
    if (strncasecmp($mainstr, $substr, strlen($substr)) === 0) {
        return substr($mainstr, strlen($substr));
    }
    return $mainstr;
}

/**
 * Make sure that a key exists in an array.
 *
 * @param string|int $key The array key to ensure.
 * @param array &$array The array to modify.
 * @param mixed $default The default value to set if key does not exist.
 * @category Array Functions
 */
function array_touch($key, &$array, $default) {
    if (!array_key_exists($key, $array)) {
        $array[$key] = $default;
    }
}

/**
 * Search an array for a value with a user-defined comparison function.
 *
 * @param mixed $needle The value to search for.
 * @param array $haystack The array to search.
 * @param callable $cmp The comparison function to use in the search.
 * @return mixed|false Returns the found value or false if the value is not found.
 */
function array_usearch($needle, array $haystack, callable $cmp) {
    $found = array_uintersect($haystack, [$needle], $cmp);

    if (empty($found)) {
        return false;
    } else {
        return array_pop($found);
    }
}

/**
 * Converts a quick array into a key/value form.
 *
 * @param array $array The array to work on.
 * @param mixed $default The default value for unspecified keys.
 * @return array Returns the array converted to long syntax.
 */
function array_quick(array $array, $default) {
    $result = [];
    foreach ($array as $key => $value) {
        if (is_int($key)) {
            $result[$value] = $default;
        } else {
            $result[$key] = $value;
        }
    }
    return $result;
}

/**
 * Force a value into a boolean.
 *
 * @param mixed $value The value to force.
 * @return boolean Returns the boolean value of {@link $value}.
 * @category Type Functions
 */
function force_bool($value) {
    if (is_string($value)) {
        switch (strtolower($value)) {
            case 'disabled':
            case 'false':
            case 'no':
            case 'off':
            case '':
                return false;
        }
        return true;
    }
    return (bool)$value;
}

/**
 * Return the value from an associative array.
 *
 * This function differs from val() in that $key can be an array that will be used to walk a nested array.
 *
 * @param array|string $keys The keys or property names of the value. This can be an array or dot-seperated string.
 * @param array|object $array The array or object to search.
 * @param mixed $default The value to return if the key does not exist.
 * @return mixed The value from the array or object.
 * @category Array Functions
 */
function valr($keys, $array, $default = null) {
    if (is_string($keys)) {
        $keys = explode('.', $keys);
    }

    $value = $array;
    for ($i = 0; $i < count($keys); ++$i) {
        $SubKey = $keys[$i];

        if (is_array($value) && isset($value[$SubKey])) {
            $value = $value[$SubKey];
        } elseif (is_object($value) && isset($value->$SubKey)) {
            $value = $value->$SubKey;
        } else {
            return $default;
        }
    }
    return $value;
}

/**
 * Force a value to be an integer.
 *
 * @param mixed $value The value to force.
 * @return int Returns the integer value of {@link $value}.
 * @category Type Functions
 */
function force_int($value) {
    if (is_string($value)) {
        switch (strtolower($value)) {
            case 'disabled':
            case 'false':
            case 'no':
            case 'off':
            case '':
                return 0;
            case 'enabled':
            case 'true':
            case 'yes':
            case 'on':
                return 1;
        }
    }
    return intval($value);
}