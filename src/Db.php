<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;

/**
 * Defines a standard set of methods that all database drivers must conform to.
 */
abstract class Db {
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
    const OPTION_FETCH_MODE = 'fetchMode';

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

    /**
     * @var string[] Maps PDO drivers to db classes.
     */
    private static $drivers = [
        'mysql' => MySqlDb::class,
        'sqlite' => SqliteDb::class
    ];

    private static $types = [

    ];

    /**
     * @var string The database prefix.
     */
    private $px = '';

    /**
     * @var array A cached copy of the table schemas indexed by lowercase name.
     */
    private $tables = [];

    /**
     * @var array A cached copy of the table names indexed by lowercase name.
     */
    private $tableNames = null;

    /**
     * @var \PDO
     */
    private $pdo;

    /**
     * @var array The default fetch mode.
     */
    private $defaultFetchMode = 0;

    /**
     * Initialize an instance of the {@link MySqlDb} class.
     *
     * @param PDO $pdo The connection to the database.
     * @param string $px The database prefix.
     */
    public function __construct(PDO $pdo, $px = '') {
        $this->pdo = $pdo;
        $this->px = $px;

        $fetchMode = $this->pdo->getAttribute(PDO::ATTR_DEFAULT_FETCH_MODE);
        $this->setDefaultFetchMode($fetchMode === PDO::FETCH_BOTH ? PDO::FETCH_ASSOC: $fetchMode);
    }

    /**
     * Get the name of the class that handles a database driver.
     *
     * @param string|PDO $driver The name of the driver or a database connection.
     * @return null|string Returns the driver classname or **null** if one isn't found.
     */
    public static function driverClass($driver) {
        if ($driver instanceof PDO) {
            $name = $driver->getAttribute(PDO::ATTR_DRIVER_NAME);
        } else {
            $name = (string)$driver;
        }

        $name = strtolower($name);
        return isset(self::$drivers[$name]) ? self::$drivers[$name] : null;
    }

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
     * @param string $table The name of the table to drop.
     * @param array $options An array of additional options when adding the table.
     */
    final public function dropTable($table, array $options = []) {
        $options += [Db::OPTION_IGNORE => false];
        $this->dropTableDb($table, $options);

        $tableKey = strtolower($table);
        unset($this->tables[$tableKey], $this->tableNames[$tableKey]);
    }

    /**
     * Perform the actual table drop.
     *
     * @param string $table The name of the table to drop.
     * @param array $options An array of additional options when adding the table.
     */
    abstract protected function dropTableDb($table, array $options = []);

    /**
     * Get the names of all the tables in the database.
     *
     * @return string[] Returns an array of table names without prefixes.
     */
    final public function getTableNames() {
        if ($this->tableNames !== null) {
            return array_values($this->tableNames);
        }

        $names = $this->getTableNamesDb();

        foreach ($names as $name) {
            $name = $this->stripPrefix($name);
            $this->tableNames[strtolower($name)] = $name;
        }

        return array_values($this->tableNames);
    }

    /**
     * Fetch the table names from the underlying database layer.
     *
     * The driver should return all table names. It doesn't have to strip the prefix.
     *
     * @return string[]
     */
    abstract protected function getTableNamesDb();

    /**
     * Get a table definition.
     *
     * @param string $table The name of the table.
     * @return array|null Returns the table definition or null if the table does not exist.
     */
    final public function getTableDef($table) {
        $tableKey = strtolower($table);

        // First check the table cache.
        if (isset($this->tables[$tableKey])) {
            $tableDef = $this->tables[$tableKey];

            if (isset($tableDef['columns'], $tableDef['indexes'])) {
                return $tableDef;
            }
        } elseif ($this->tableNames !== null && !isset($this->tableNames[$tableKey])) {
            return null;
        }

        $tableDef = $this->getTableDefDb($table);
        if ($tableDef !== null) {
            $this->fixIndexes($tableDef['name'], $tableDef);
            $this->tables[$tableKey] = $tableDef;
        }

        return $tableDef;
    }

    /**
     * Fetch the table definition from the database.
     *
     * @param string $table The name of the table to get.
     * @return array|null Returns the table def or **null** if the table doesn't exist.
     */
    abstract protected function getTableDefDb($table);


    /**
     * Get the column definitions for a table.
     *
     * @param string $table The name of the table to get the columns for.
     * @return array|null Returns an array of column definitions.
     */
    final public function getColumnDefs($table) {
        $tableKey = strtolower($table);

        if (!empty($this->tables[$tableKey]['columns'])) {
            $this->tables[$tableKey]['columns'];
        } elseif ($this->tableNames !== null && !isset($this->tableNames[$tableKey])) {
            return null;
        }

        $columnDefs = $this->getColumnDefs($table);
        if ($columnDefs !== null) {
            $this->tables[$tableKey]['columns'] = $columnDefs;
        }
        return $columnDefs;
    }

    /**
     * Get the column definitions from the database.
     *
     * @param string $table The name of the table to fetch the columns for.
     * @return array|null
     */
    abstract protected function getColumnDefsDb($table);

    /**
     * Set a table definition to the database.
     *
     * @param array $tableDef The table definition.
     * @param array $options An array of additional options when adding the table.
     */
    final public function defineTable(array $tableDef, array $options = []) {
        $options += [Db::OPTION_DROP => false];

        $tableName = $tableDef['name'];
        $tableKey = strtolower($tableName);
        $tableDef['name'] = $tableName;
        $curTable = $this->getTableDef($tableName);

        $this->fixIndexes($tableName, $tableDef, $curTable);

        if (!$curTable) {
            $this->createTable($tableDef, $options);
            $this->tables[$tableKey] = $tableDef;
            $this->tableNames[$tableKey] = $tableDef['name'];
            return;
        }
        // This is the alter statement.
        $alterDef = ['name' => $tableName];

        // Figure out the columns that have changed.
        $curColumns = (array)$curTable['columns'];
        $newColumns = (array)$tableDef['columns'];

        $alterDef['add']['columns'] = array_diff_key($newColumns, $curColumns);
        $alterDef['alter']['columns'] = array_uintersect_assoc($newColumns, $curColumns, function ($new, $curr) {
            $search = ['dbtype', 'allowNull', 'default'];
            foreach ($search as $key) {
                if (self::val($key, $curr) !== self::val($key, $new)) {
                    // Return 0 if the values are different, not the same.
                    return 0;
                }
            }
            return 1;
        });

        // Figure out the indexes that have changed.
        $curIndexes = (array)self::val('indexes', $curTable, []);
        $newIndexes = (array)self::val('indexes', $tableDef, []);

        $alterDef['add']['indexes'] = array_udiff($newIndexes, $curIndexes, [$this, 'indexCompare']);

        $dropIndexes = array_udiff($curIndexes, $newIndexes, [$this, 'indexCompare']);
        if ($options[Db::OPTION_DROP]) {
            $alterDef['drop']['columns'] = array_diff_key($curColumns, $newColumns);
            $alterDef['drop']['indexes'] = $dropIndexes;
        } else {
            $alterDef['drop']['columns'] = [];
            $alterDef['drop']['indexes'] = [];

            // If the primary key has changed then the old one needs to be dropped.
            if ($pk = $this->findPrimaryKeyIndex($dropIndexes)) {
                $alterDef['drop']['indexes'][] = $pk;
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
        $this->tables[$tableKey] = $tableDef;
        $this->tableNames[$tableKey] = $tableName;
    }

    /**
     * Find the primary key in an array of indexes.
     *
     * @param array $indexes The indexes to search.
     * @return array|null Returns the primary key or **null** if there isn't one.
     */
    protected function findPrimaryKeyIndex(array $indexes) {
        foreach ($indexes as $index) {
            if ($index['type'] === Db::INDEX_PK) {
                return $index;
            }
        }
        return null;
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
        $tableDef += ['indexes' => []];

        // Loop through the columns and add the primary key index.
        $primaryColumns = [];
        foreach ($tableDef['columns'] as $cname => $cdef) {
            if (!empty($cdef['primary'])) {
                $primaryColumns[] = $cname;
            }
        }

        // Massage the primary key index.
        $primaryFound = false;
        foreach ($tableDef['indexes'] as &$indexDef) {
            $indexDef += ['name' => $this->buildIndexName($tableName, $indexDef), 'type' => null];

            if ($indexDef['type'] === Db::INDEX_PK) {
                $primaryFound = true;

                if (empty($primaryColumns)) {
                    foreach ($indexDef['columns'] as $cname) {
                        $tableDef['columns'][$cname]['primary'] = true;
                    }
                } elseif (array_diff($primaryColumns, $indexDef['columns'])) {
                    throw new \Exception("There is a mismatch in the primary key index and primary key columns.", 500);
                }
            } elseif (isset($curTableDef['indexes'])) {
                foreach ($curTableDef['indexes'] as $curIndexDef) {
                    if ($this->indexCompare($indexDef, $curIndexDef) === 0) {
                        if (!empty($curIndexDef['name'])) {
                            $indexDef['name'] = $curIndexDef['name'];
                        }
                        break;
                    }
                }
            }
        }

        if (!$primaryFound && !empty($primaryColumns)) {
            $tableDef['indexes'][] = [
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
    private function indexCompare(array $a, array $b) {
        if ($a['columns'] > $b['columns']) {
            return 1;
        } elseif ($a['columns'] < $b['columns']) {
            return -1;
        }

        return strcmp(
            isset($a['type']) ? $a['type'] : '',
            isset($b['type']) ? $b['type'] : ''
        );
    }

    /**
     * Get data from the database.
     *
     * @param string|Identifier $table The name of the table to get the data from.
     * @param array $where An array of where conditions.
     * @param array $options An array of additional options.
     * @return \PDOStatement Returns the result set.
     */
    abstract public function get($table, array $where, array $options = []);

    /**
     * Get a single row from the database.
     *
     * This is a convenience method that calls {@link Db::get()} and shifts off the first row.
     *
     * @param string|Identifier $table The name of the table to get the data from.
     * @param array $where An array of where conditions.
     * @param array $options An array of additional options.
     * @return array|object|null Returns the row or false if there is no row.
     */
    final public function getOne($table, array $where, array $options = []) {
        $options['limit'] = 1;
        $rows = $this->get($table, $where, $options);
        $row = $rows->fetch();

        return $row === false ? null : $row;
    }

    /**
     * Insert a row into a table.
     *
     * @param string $table The name of the table to insert into.
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
     * @return mixed Returns the id of the inserted record, **true** if the table doesn't have an auto increment, or **false** otherwise.
     * @see Db::load()
     */
    abstract public function insert($table, array $row, array $options = []);

    /**
     * Load many rows into a table.
     *
     * @param string $table The name of the table to insert into.
     * @param \Traversable|array $rows A dataset to insert.
     * Note that all rows must contain the same columns.
     * The first row will be looked at for the structure of the insert and the rest of the rows will use this structure.
     * @param array $options An array of options for the inserts. See {@link Db::insert()} for details.
     * @see Db::insert()
     */
    public function load($table, $rows, array $options = []) {
        foreach ($rows as $row) {
            $this->insert($table, $row, $options);
        }
    }


    /**
     * Update a row or rows in a table.
     *
     * @param string $table The name of the table to update.
     * @param array $set The values to set.
     * @param array $where The where filter for the update.
     * @param array $options An array of options for the update.
     * @return int Returns the number of affected rows.
     */
    abstract public function update($table, array $set, array $where, array $options = []);

    /**
     * Delete rows from a table.
     *
     * @param string $table The name of the table to delete from.
     * @param array $where The where filter of the delete.
     * @param array $options An array of options.
     *
     * Db:OPTION_TRUNCATE
     * : Truncate the table instead of deleting rows. In this case {@link $where} must be blank.
     * @return int Returns the number of affected rows.
     */
    abstract public function delete($table, array $where, array $options = []);

    /**
     * Reset the internal table definition cache.
     *
     * @return $this
     */
    public function reset() {
        $this->tables = [];
        $this->tableNames = null;
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
        $indexDef += ['type' => Db::INDEX_IX, 'suffix' => ''];

        $type = $indexDef['type'];

        if ($type === Db::INDEX_PK) {
            return 'primary';
        }
        $px = self::val($type, [Db::INDEX_IX => 'ix_', Db::INDEX_UNIQUE => 'ux_'], 'ix_');
        $sx = $indexDef['suffix'];
        $result = $px.$tableName.'_'.($sx ?: implode('', $indexDef['columns']));
        return $result;
    }

    /**
     * Execute a query that fetches data.
     *
     * @param string $sql The query to execute.
     * @param array $params Input parameters for the query.
     * @param array $options Additional options.
     * @return \PDOStatement Returns the result of the query.
     * @throws \PDOException Throws an exception if something went wrong during the query.
     */
    protected function queryStatement($sql, array $params = [], array $options = []) {
        $options += [Db::OPTION_FETCH_MODE => $this->defaultFetchMode];

        $stm = $this->getPDO()->prepare($sql);
        $r = $stm->execute($params);

        if ($options[Db::OPTION_FETCH_MODE]) {
            $stm->setFetchMode(...(array)$options[Db::OPTION_FETCH_MODE]);
        }

        // This is a kludge for those that don't have errors turning into exceptions.
        if ($r === false) {
            list($state, $code, $msg) = $stm->errorInfo();
            throw new \PDOException($msg, $code);
        }

        return $stm;
    }

    /**
     * Query the database and return a row count.
     *
     * @param string $sql The query to execute.
     * @param array $params Input parameters for the query.
     * @param array $options Additional options.
     * @return int
     */
    protected function queryModify($sql, array $params = [], array $options = []) {
        $stm = $this->queryStatement($sql, $params, $options);
        return $stm->rowCount();
    }

    /**
     * Query the database and return the ID of the record that was inserted.
     *
     * @param string $sql The query to execute.
     * @param array $params Input parameters for the query.
     * @param array $options Additional options.
     * @return int Returns the record ID.
     */
    protected function queryID($sql, array $params = [], array $options = []) {
        $stm = $this->queryStatement($sql, $params, $options);
        $r = $this->getPDO()->lastInsertId();
        return (int)$r;
    }

    /**
     * Query the database for a database define.
     *
     * @param string $sql The query to execute.
     * @param array $options Additional options.
     */
    protected function queryDefine($sql, array $options = []) {
        $stm = $this->queryStatement($sql, [], $options);
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

    /**
     * Escape an identifier.
     *
     * @param string|Literal $identifier The identifier to escape.
     * @return string Returns the field properly escaped.
     */
    public function escape($identifier) {
        if ($identifier instanceof Literal) {
            return $identifier->getValue($this);
        }
        return '`'.str_replace('`', '``', $identifier).'`';
    }

    /**
     * Escape a a like string so that none of its characters work as wildcards.
     *
     * @param string $str The string to escape.
     * @return string Returns an escaped string.
     */
    protected function escapeLike($str) {
        return addcslashes($str, '_%');
    }

    /**
     * Prefix a table name.
     *
     * @param string|Identifier $table The name of the table to prefix.
     * @param bool $escape Whether or not to escape the output.
     * @return string Returns a full table name.
     */
    protected function prefixTable($table, $escape = true) {
        if ($table instanceof Identifier) {
            return $escape ? $table->escape($this) : (string)$table;
        } else {
            $table = $this->px.$table;
            return $escape ? $this->escape($table) : $table;
        }
    }

    /**
     * Strip the database prefix off a table name.
     *
     * @param string $table The name of the table to strip.
     * @return string Returns the table name stripped of the prefix.
     */
    protected function stripPrefix($table) {
        $len = strlen($this->px);
        if (strcasecmp(substr($table, 0, $len), $this->px) === 0) {
            $table = substr($table, $len);
        }
        return $table;
    }

    /**
     * Optionally quote a where value.
     *
     * @param mixed $value The value to quote.
     * @param string $column The column being operated on. It must already be quoted.
     * @return string Returns the value, optionally quoted.
     * @internal param bool $quote Whether or not to quote the value.
     */
    public function quote($value, $column = '') {
        if ($value instanceof Literal) {
            /* @var Literal $value */
            return $value->getValue($this, $column);
        } else {
            return $this->getPDO()->quote($value);
        }
    }

    /**
     * Gets the {@link PDO} object for this connection.
     *
     * @return \PDO
     */
    public function getPDO() {
        return $this->pdo;
    }

    /**
     * Set the connection to the database.
     *
     * @param PDO $pdo The new connection to the database.
     * @return $this
     */
    public function setPDO(PDO $pdo) {
        $this->pdo = $pdo;
        return $this;
    }

    /**
     * Get the default fetch mode.
     *
     * @return array|int Returns the default fetch mode.
     */
    public function getDefaultFetchMode() {
        if (count($this->defaultFetchMode) === 1) {
            return $this->defaultFetchMode[0];
        } else {
            return $this->defaultFetchMode;
        }
    }

    /**
     * Set the defaultFetchMode.
     *
     * @param array $mode
     * @return $this
     */
    public function setDefaultFetchMode(...$mode) {
        $this->defaultFetchMode = $mode;
        return $this;
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
