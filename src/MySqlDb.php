<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;

/**
 * A {@link Db} class for connecting to MySQL.
 */
class MySqlDb extends Db {
    /**
     * @var string
     */
    protected $dbname;

    protected static $map = [
        Db::OP_GT => '>',
        Db::OP_GTE => '>=',
        Db::OP_LT => '<',
        Db::OP_LTE => '<=',
        Db::OP_LIKE => 'like',
        Db::OP_AND => 'and',
        Db::OP_OR => 'or',

        Db::ORDER_ASC => 'asc',
        Db::ORDER_DESC => 'desc'
    ];

    /**
     * {@inheritdoc}
     */
    public function dropTable($tableName, array $options = []) {
        $sql = 'drop table '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'if exists ' : '').
            $this->prefixTable($tableName);
        $result = $this->query($sql, Db::QUERY_DEFINE);
        unset($this->tables[strtolower($tableName)]);

        return $result;
    }

    /**
     * Execute a query on the database.
     *
     * @param string $sql The sql query to execute.
     * @param string $type One of the Db::QUERY_* constants.
     *
     * Db::QUERY_READ
     * : The query reads from the database.
     *
     * Db::QUERY_WRITE
     * : The query writes to the database.
     *
     * Db::QUERY_DEFINE
     * : The query alters the structure of the datbase.
     *
     * @param array $options Additional options for the query.
     *
     * Db::OPTION_MODE
     * : Override {@link Db::$mode}.
     *
     * @return array|string|PDOStatement|int Returns the result of the query.
     *
     * array
     * : Returns an array when reading from the database and the mode is {@link Db::MODE_EXEC}.
     * string
     * : Returns the sql query when the mode is {@link Db::MODE_SQL}.
     * PDOStatement
     * : Returns a {@link \PDOStatement} when the mode is {@link Db::MODE_PDO}.
     * int
     * : Returns the number of rows affected when performing an update or an insert.
     */
    public function query($sql, $type = Db::QUERY_READ, $options = []) {
        $options += [
            Db::OPTION_MODE => $this->mode
        ];
        $mode = $options[Db::OPTION_MODE];

        if ($mode & Db::MODE_ECHO) {
            echo trim($sql, "\n;").";\n\n";
        }
        if ($mode & Db::MODE_SQL) {
            return $sql;
        }

        $result = null;
        if ($mode & Db::MODE_EXEC) {
            $result = $this->getPDO()->query($sql);

            if ($type == Db::QUERY_READ) {
                $result->setFetchMode(PDO::FETCH_ASSOC);
                $result = $result->fetchAll();
                $this->rowCount = count($result);
            } elseif (is_object($result) && method_exists($result, 'rowCount')) {
                $this->rowCount = $result->rowCount();
                $result = $this->rowCount;
            }
        } elseif ($mode & Db::MODE_PDO) {
            /* @var \PDOStatement $result */
            $result = $this->getPDO()->prepare($sql);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function getTableDef($table) {
        $tableDef = parent::getTableDef($table);
        if ($tableDef || $tableDef === null) {
            return $tableDef;
        }

        $ltablename = strtolower($table);
        $tableDef = self::val($ltablename, $this->tables, []);
        if (!isset($tableDef['columns'])) {
            $columns = $this->getColumns($table);
            if ($columns === null) {
                // A table with no columns does not exist.
                $this->tables[$ltablename] = ['name' => $table];
                return null;
            }

            $tableDef['columns'] = $columns;
        }
        if (!isset($tableDef['indexes'])) {
            $tableDef['indexes'] = $this->getIndexes($table);
        }
        $tableDef['name'] = $table;
        $this->tables[$ltablename] = $tableDef;
        return $tableDef;
    }

    /**
     * Get the columns for tables and put them in {MySqlDb::$tables}.
     *
     * @param string $tableName The table to get the columns for or blank for all columns.
     * @return array|null Returns an array of columns if {@link $tablename} is specified, or null otherwise.
     */
    protected function getColumns($tableName = '') {
        $ltablename = strtolower($tableName);
        /* @var \PDOStatement $stmt */
        $stmt = $this->get(
            new Literal('information_schema.COLUMNS'),
            [
                'TABLE_SCHEMA' => $this->getDbName(),
                'TABLE_NAME' => $tableName ? $this->getPx().$tableName : [Db::OP_LIKE => $this->escapeLike($this->getPx()).'%']
            ],
            [
                'columns' => [
                    'TABLE_NAME',
                    'COLUMN_TYPE',
                    'IS_NULLABLE',
                    'EXTRA',
                    'COLUMN_KEY',
                    'COLUMN_DEFAULT',
                    'COLUMN_NAME'
                ],
                Db::OPTION_MODE => Db::MODE_PDO,
                'order' => ['TABLE_NAME', 'ORDINAL_POSITION']
            ]
        );

        $stmt->execute();
        $tablecolumns = $stmt->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

        foreach ($tablecolumns as $ctablename => $cdefs) {
            $ctablename = strtolower(ltrim_substr($ctablename, $this->getPx()));
            $columns = [];

            foreach ($cdefs as $cdef) {
                $column = [
                    'dbtype' => $this->columnTypeString($cdef['COLUMN_TYPE']),
                    'allowNull' => force_bool($cdef['IS_NULLABLE']),
                ];
                if ($cdef['EXTRA'] === 'auto_increment') {
                    $column['autoIncrement'] = true;
                }
                if ($cdef['COLUMN_KEY'] === 'PRI') {
                    $column['primary'] = true;
                }

                if ($cdef['COLUMN_DEFAULT'] !== null) {
                    $column['default'] = $this->forceType($cdef['COLUMN_DEFAULT'], $column['dbtype']);
                }

                $columns[$cdef['COLUMN_NAME']] = $column;
            }
            $this->tables[$ctablename]['columns'] = $columns;
        }
        if ($ltablename && isset($this->tables[$ltablename]['columns'])) {
            return $this->tables[$ltablename]['columns'];
        }
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function get($tableName, array $where, array $options = []) {
        $sql = $this->buildSelect($tableName, $where, $options);
        $result = $this->query($sql, Db::QUERY_READ, $options);
        return $result;
    }

    /**
     * Build a sql select statement.
     *
     * @param string|Literal $tableName The name of the main table.
     * @param array $where The where filter.
     * @param array $options An array of additional query options.
     * @return string Returns the select statement as a string.
     * @see Db::get()
     */
    protected function buildSelect($tableName, array $where, array $options = []) {
        $sql = '';

        // Build the select clause.
        if (isset($options['columns'])) {
            $columns = array();
            foreach ($options['columns'] as $value) {
                $columns[] = $this->escape($value);
            }
            $sql .= 'select '.implode(', ', $columns);
        } else {
            $sql .= "select *";
        }

        // Build the from clause.
        if ($tableName instanceof Literal) {
            $tableName = $tableName->getValue($this);
        } else {
            $tableName = $this->prefixTable($tableName);
        }
        $sql .= "\nfrom $tableName";

        // Build the where clause.
        $whereString = $this->buildWhere($where, Db::OP_AND);
        if ($whereString) {
            $sql .= "\nwhere ".$whereString;
        }

        // Build the order.
        if (isset($options['order'])) {
            $order = array_quick($options['order'], Db::ORDER_ASC);
            $orders = array();
            foreach ($order as $key => $value) {
                switch ($value) {
                    case Db::ORDER_ASC:
                    case Db::ORDER_DESC:
                        $direction = self::$map[$value];

                        $orders[] = $this->escape($key)." $direction";
                        break;
                    default:
                        trigger_error("Invalid sort direction '$value' for column '$key'.", E_USER_WARNING);
                }
            }

            $sql .= "\norder by ".implode(', ', $orders);
        }

        // Build the limit, offset.
        $limit = 10;
        if (isset($options['limit'])) {
            $limit = (int)$options['limit'];
            $sql .= "\nlimit $limit";
        }

        if (isset($options['offset'])) {
            $sql .= ' offset '.((int)$options['offset']);
        } elseif (isset($options['page'])) {
            $offset = $limit * ($options['page'] - 1);
            $sql .= ' offset '.$offset;
        }

        return $sql;
    }

    /**
     * Build a where clause from a where array.
     *
     * @param array $where There where string.
     * This is an array in the form `['column' => 'value']` with more advanced options for non-equality comparisons.
     * @param string $op The logical operator to join multiple field comparisons.
     * @return string The where string.
     */
    protected function buildWhere($where, $op = Db::OP_AND) {
        $map = static::$map;
        $strop = $map[$op];

        $result = '';
        foreach ($where as $column => $value) {
            $btcolumn = $this->escape($column);

            if (is_array($value)) {
                if (is_numeric($column)) {
                    // This is a bracketed expression.
                    $result .= (empty($result) ? '' : "\n  $strop ").
                        "(\n  ".
                        $this->buildWhere($value, $op).
                        "\n  )";
                } elseif (in_array($column, [Db::OP_AND, Db::OP_OR])) {
                    // This is an AND/OR expression.
                    $result .= (empty($result) ? '' : "\n  $strop ").
                        "(\n  ".
                        $this->buildWhere($value, $column).
                        "\n  )";
                } else {
                    if (isset($value[0])) {
                        // This is a short in syntax.
                        $value = [Db::OP_IN => $value];
                    }

                    foreach ($value as $vop => $rval) {
                        if ($result) {
                            $result .= "\n  $strop ";
                        }

                        switch ($vop) {
                            case Db::OP_AND:
                            case Db::OP_OR:
                                if (is_numeric($column)) {
                                    $innerWhere = $rval;
                                } else {
                                    $innerWhere = [$column => $rval];
                                }
                                $result .= "(\n  ".
                                    $this->buildWhere($innerWhere, $vop).
                                    "\n  )";
                                break;
                            case Db::OP_EQ:
                                if ($rval === null) {
                                    $result .= "$btcolumn is null";
                                } elseif (is_array($rval)) {
                                    $result .= "$btcolumn in ".$this->bracketList($rval);
                                } else {
                                    $result .= "$btcolumn = ".$this->quote($rval);
                                }
                                break;
                            case Db::OP_GT:
                            case Db::OP_GTE:
                            case Db::OP_LT:
                            case Db::OP_LTE:
                                $result .= "$btcolumn {$map[$vop]} ".$this->quote($rval);
                                break;
                            case Db::OP_LIKE:
                                $result .= $this->buildLike($btcolumn, $rval);
                                break;
                            case Db::OP_IN:
                                // Quote the in values.
                                $rval = array_map([$this, 'quote'], (array)$rval);
                                $result .= "$btcolumn in (".implode(', ', $rval).')';
                                break;
                            case Db::OP_NEQ:
                                if ($rval === null) {
                                    $result .= "$btcolumn is not null";
                                } elseif (is_array($rval)) {
                                    $result .= "$btcolumn not in ".$this->bracketList($rval);
                                } else {
                                    $result .= "$btcolumn <> ".$this->quote($rval);
                                }
                                break;
                        }
                    }
                }
            } else {
                if ($result) {
                    $result .= "\n  $strop ";
                }

                // This is just an equality operator.
                if ($value === null) {
                    $result .= "$btcolumn is null";
                } else {
                    $result .= "$btcolumn = ".$this->quote($value);
                }
            }
        }
        return $result;
    }

    /**
     * Build a like expression.
     *
     * @param string $column The column name.
     * @param mixed $value The right-hand value.
     * @return string Returns the like expression.
     * @internal param bool $quotevals Whether or not to quote the values.
     */
    protected function buildLike($column, $value) {
        return "$column like ".$this->quote($value);
    }

    /**
     * Convert an array into a bracketed list suitable for MySQL clauses.
     *
     * @param array $row The row to expand.
     * @param string $quote The quotes to surroud the items with. There are two special cases.
     * ' (single quote)
     * : The row will be passed through {@link PDO::quote()}.
     * ` (backticks)
     * : The row will be passed through {@link MySqlDb::backtick()}.
     * @return string Returns the bracket list.
     */
    public function bracketList($row, $quote = "'") {
        switch ($quote) {
            case "'":
                $row = array_map([$this, 'quote'], $row);
                $quote = '';
                break;
            case '`':
                $row = array_map([$this, 'escape'], $row);
                $quote = '';
                break;
        }

        return "($quote".implode("$quote, $quote", $row)."$quote)";
    }


    /**
     * Get the current database name.
     *
     * @return mixed
     */
    private function getDbName() {
        if (!isset($this->dbname)) {
            $this->dbname = $this->getPDO()->query('select database()')->fetchColumn();
        }
        return $this->dbname;
    }

    /**
     * Parse a column type string and return it in a way that is suitable for a create/alter table statement.
     *
     * @param string $typeString The string to parse.
     * @return string Returns a canonical string.
     */
    protected function columnTypeString($typeString) {
        $type = null;

        if (substr($typeString, 0, 4) === 'enum') {
            // This is an enum which will come in as an array.
            if (preg_match_all("`'([^']+)'`", $typeString, $matches)) {
                $type = $matches[1];
            }
        } else {
            if (preg_match('`([a-z]+)\s*(?:\((\d+(?:\s*,\s*\d+)*)\))?\s*(unsigned)?`', $typeString, $matches)) {
                //         var_dump($matches);
                $str = $matches[1];
                $length = self::val(2, $matches);
                $unsigned = self::val(3, $matches);

                if (substr($str, 0, 1) == 'u') {
                    $unsigned = true;
                    $str = substr($str, 1);
                }

                // Remove the length from types without real lengths.
                if (in_array($str, array('tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'float', 'double'))) {
                    $length = null;
                }

                $type = $str;
                if ($length) {
                    $length = str_replace(' ', '', $length);
                    $type .= "($length)";
                }
                if ($unsigned) {
                    $type .= ' unsigned';
                }
            }
        }

        if (!$type) {
            debug_print_backtrace();
            trigger_error("Couldn't parse type $typeString", E_USER_ERROR);
        }

        return $type;
    }

    /**
     * Get the indexes from the database.
     *
     * @param string $tableName The name of the table to get the indexes for or an empty string to get all indexes.
     * @return array|null
     */
    protected function getIndexes($tableName = '') {
        $ltablename = strtolower($tableName);
        /* @var \PDOStatement */
        $stmt = $this->get(
            new Literal('information_schema.STATISTICS'),
            [
                'TABLE_SCHEMA' => $this->getDbName(),
                'TABLE_NAME' => $tableName ? $this->getPx().$tableName : [Db::OP_LIKE => $this->escapeLike($this->getPx()).'%']
            ],
            [
                'columns' => [
                    'INDEX_NAME',
                    'TABLE_NAME',
                    'NON_UNIQUE',
                    'COLUMN_NAME'
                ],
                'order' => ['TABLE_NAME', 'INDEX_NAME', 'SEQ_IN_INDEX'],
                Db::OPTION_MODE => Db::MODE_PDO
            ]
        );

        $stmt->execute();
        $indexDefs = $stmt->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

        foreach ($indexDefs as $indexName => $indexRows) {
            $row = reset($indexRows);
            $itablename = strtolower(ltrim_substr($row['TABLE_NAME'], $this->getPx()));
            $index = [
                'name' => $indexName,
                'columns' => array_column($indexRows, 'COLUMN_NAME')
            ];

            if ($indexName === 'PRIMARY') {
                $index['type'] = Db::INDEX_PK;
                $this->tables[$itablename]['indexes'][Db::INDEX_PK] = $index;
            } else {
                $index['type'] = $row['NON_UNIQUE'] ? Db::INDEX_IX : Db::INDEX_UNIQUE;
                $this->tables[$itablename]['indexes'][] = $index;
            }
        }

        if ($ltablename) {
            return valr([$ltablename, 'indexes'], $this->tables, []);
        }
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function getAllTables($withDefs = false) {
        $tables = parent::getAllTables($withDefs);
        if ($tables !== null) {
            return $tables;
        }

        // Grab the table names first.
        if ($this->allTablesFetched & Db::FETCH_TABLENAMES) {
            $tablenames = array_keys($this->tables);
        } else {
            $tablenames = $this->getTableNames();
            $this->tables = [];
            foreach ($tablenames as $tablename) {
                $this->tables[strtolower($tablename)] = ['name' => $tablename];
            }
            $this->allTablesFetched = Db::FETCH_TABLENAMES;
        }

        if (!$withDefs) {
            return $tablenames;
        }

        $this->getColumns();
        $this->allTablesFetched |= Db::FETCH_COLUMNS;

        $this->getIndexes();
        $this->allTablesFetched |= Db::FETCH_INDEXES;

        return $this->tables;
    }

    /**
     * Get the all of tablenames in the database.
     *
     * @return array Returns an array of table names with prefixes stripped.
     */
    protected function getTableNames() {
        // Get the table names.
        $tables = (array)$this->get(
            new Escaped('information_schema', 'TABLES'),
            [
                'TABLE_SCHEMA' => $this->getDbName(),
                'TABLE_NAME' => [Db::OP_LIKE => $this->escapeLike($this->getPx()).'%']
            ],
            [
                'columns' => ['TABLE_NAME']
            ]
        );

        // Strip the table prefixes.
        $tables = array_map(function ($name) {
            return ltrim_substr($name, $this->getPx());
        }, array_column($tables, 'TABLE_NAME'));

        return $tables;
    }

    /**
     * {@inheritdoc}
     */
    public function insert($tableName, array $rows, array $options = []) {
        $sql = $this->buildInsert($tableName, $rows, $options);
        $this->query($sql, Db::QUERY_WRITE);
        $id = $this->getPDO()->lastInsertId();
        if (is_numeric($id)) {
            return (int)$id;
        } else {
            return $id;
        }
    }

    /**
     * Build an insert statement.
     *
     * @param string|Literal $tableName The name of the table to insert to.
     * @param array $row The row to insert.
     * @param array $options An array of options for the insert. See {@link Db::insert} for the options.
     * @return string Returns the the sql string of the insert statement.
     */
    protected function buildInsert($tableName, array $row, $options = []) {
        if (self::val(Db::OPTION_UPSERT, $options)) {
            return $this->buildUpsert($tableName, $row, $options);
        } elseif (self::val(Db::OPTION_IGNORE, $options)) {
            $sql = 'insert ignore ';
        } elseif (self::val(Db::OPTION_REPLACE, $options)) {
            $sql = 'replace ';
        } else {
            $sql = 'insert ';
        }
        $sql .= $this->prefixTable($tableName);

        // Add the list of values.
        $sql .=
            "\n".$this->bracketList(array_keys($row), '`').
            "\nvalues".$this->bracketList($row, "'");

        return $sql;
    }

    /**
     * Build an upsert statement.
     *
     * An upsert statement is an insert on duplicate key statement in MySQL.
     *
     * @param string $tableName The name of the table to update.
     * @param array $row The row to insert or update.
     * @param array $options An array of additional query options.
     * @return string Returns the upsert statement as a string.
     */
    protected function buildUpsert($tableName, array $row, $options = []) {
        // Build the initial insert statement first.
        unset($options[Db::OPTION_UPSERT]);
        $sql = $this->buildInsert($tableName, $row, $options);

        // Add the duplicate key stuff.
        $updates = [];
        foreach ($row as $key => $value) {
            $updates[] = $this->escape($key).' = values('.$this->escape($key).')';
        }
        $sql .= "\non duplicate key update ".implode(', ', $updates);

        return $sql;
    }

    /**
     * {@inheritdoc}
     */
    public function load($tableName, $rows, array $options = []) {
        $count = 0;
        $first = true;
        $spec = [];
        $stmt = null;

        // Loop over the rows and insert them with the statement.
        foreach ($rows as $row) {
            if ($first) {
                // Build the insert statement from the first row.
                foreach ($row as $key => $value) {
                    $spec[$key] = new Literal($this->paramName($key));
                }

                $sql = $this->buildInsert($tableName, $spec, $options);
                $stmt = $this->getPDO()->prepare($sql);
                $first = false;
            }

            $stmt->execute($row);
            $count += $stmt->rowCount();
        }

        return $count;
    }

    /**
     * Make a valid pdo parameter name from a string.
     *
     * This method replaces invalid placeholder characters with underscores.
     *
     * @param string $name The name to replace.
     * @return string
     */
    protected function paramName($name) {
        $result = ':'.preg_replace('`[^a-zA-Z0-9_]`', '_', $name);
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function update($tableName, array $set, array $where, array $options = []) {
        $sql = $this->buildUpdate($tableName, $set, $where, $options);
        $result = $this->query($sql, Db::QUERY_WRITE);

        if ($result instanceof \PDOStatement) {
            /* @var \PDOStatement $result */
            return $result->rowCount();
        }
        return $result;
    }

    /**
     * Build a sql update statement.
     *
     * @param string|Literal $tableName The name of the table to update.
     * @param array $set An array of columns to set.
     * @param array $where The where filter.
     * @param array $options Additional options for the query.
     * @return string Returns the update statement as a string.
     */
    protected function buildUpdate($tableName, array $set, array $where, array $options = []) {
        $sql = 'update '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'ignore ' : '').
            $this->prefixTable($tableName).
            "\nset\n  ";

        $parts = [];
        foreach ($set as $key => $value) {
            $quotedKey = $this->escape($key);

            $parts[] = $quotedKey.' = '.$this->quote($value);
        }
        $sql .= implode(",\n  ", $parts);

        if (!empty($where)) {
            $sql .= "\nwhere ".$this->buildWhere($where, Db::OP_AND);
        }

        return $sql;
    }

    /**
     * {@inheritdoc}
     */
    public function delete($tableName, array $where, array $options = []) {
        if (self::val(Db::OPTION_TRUNCATE, $options)) {
            if (!empty($where)) {
                throw new \InvalidArgumentException("You cannot truncate $tableName with a where filter.", 500);
            }
            $sql = 'truncate table '.$this->prefixTable($tableName);
        } else {
            $sql = 'delete from '.$this->prefixTable($tableName);

            if (!empty($where)) {
                $sql .= "\nwhere ".$this->buildWhere($where);
            }
        }
        return $this->query($sql, Db::QUERY_WRITE);
    }

    /**
     * {@inheritdoc}
     */
    protected function createTable(array $tableDef, array $options = []) {
        $tableName = $tableDef['name'];

        // The table doesn't exist so this is a create table.
        $parts = array();
        foreach ($tableDef['columns'] as $name => $def) {
            $parts[] = $this->columnDefString($name, $def);
        }

        foreach (self::val('indexes', $tableDef, []) as $index) {
            $indexDef = $this->indexDefString($tableName, $index);
            if ($indexDef) {
                $parts[] = $indexDef;
            }
        }

        $fullTablename = $this->prefixTable($tableName);
        $sql = "create table $fullTablename (\n  ".
            implode(",\n  ", $parts).
            "\n)";

        if (self::val('collate', $options)) {
            $sql .= "\n collate {$options['collate']}";
        }

        $this->query($sql, Db::QUERY_DEFINE);
    }

    /**
     * Construct a column definition string from an array defintion.
     *
     * @param string $name The name of the column.
     * @param array $def The column definition.
     * @return string Returns a string representing the column definition.
     */
    protected function columnDefString($name, array $def) {
        $result = $this->escape($name).' '.$this->columnTypeString($def['dbtype']);

        if (!self::val('allowNull', $def)) {
            $result .= ' not null';
        }

        if (isset($def['default'])) {
            $result .= ' default '.$this->quote($def['default']);
        }

        if (self::val('autoIncrement', $def)) {
            $result .= ' auto_increment';
        }

        return $result;
    }

    /**
     * Return the SDL string that defines an index.
     *
     * @param string $tableName The name of the table that the index is on.
     * @param array $def The index defintion. This definition should have the following keys.
     *
     * columns
     * : An array of columns in the index.
     * type
     * : One of "index", "unique", or "primary".
     * @return null|string Returns the index string or null if the index is not correct.
     */
    protected function indexDefString($tableName, array $def) {
        $indexName = $this->escape($this->buildIndexName($tableName, $def));
        switch (self::val('type', $def, Db::INDEX_IX)) {
            case Db::INDEX_IX:
                return "index $indexName ".$this->bracketList($def['columns'], '`');
            case Db::INDEX_UNIQUE:
                return "unique $indexName ".$this->bracketList($def['columns'], '`');
            case Db::INDEX_PK:
                return "primary key ".$this->bracketList($def['columns'], '`');
        }
        return null;
    }

    /**
     * {@inheritdoc}
     */
    protected function alterTable(array $alterDef, array $options = []) {
        $tablename = $alterDef['name'];
        $columnOrders = $this->getColumnOrders($alterDef['def']['columns']);
        $parts = [];

        // Add the columns and indexes.
        foreach ($alterDef['add']['columns'] as $cname => $cdef) {
            // Figure out the order of the column.
            $pos = self::val($cname, $columnOrders, '');
            $parts[] = 'add '.$this->columnDefString($cname, $cdef).$pos;
        }
        foreach ($alterDef['add']['indexes'] as $ixdef) {
            $parts[] = 'add '.$this->indexDefString($tablename, $ixdef);
        }

        // Alter the columns.
        foreach ($alterDef['alter']['columns'] as $cname => $cdef) {
            $parts[] = 'modify '.$this->columnDefString($cname, $cdef);
        }

        // Drop the columns and indexes.
        foreach ($alterDef['drop']['columns'] as $cname => $_) {
            $parts[] = 'drop '.$this->escape($cname);
        }
        foreach ($alterDef['drop']['indexes'] as $ixdef) {
            $parts[] = 'drop index '.$this->escape($ixdef['name']);
        }

        if (empty($parts)) {
            return false;
        }

        $sql = 'alter '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'ignore ' : '').
            'table '.$this->prefixTable($tablename)."\n  ".
            implode(",\n  ", $parts);

        $result = $this->query($sql, Db::QUERY_DEFINE);
        return $result;
    }

    /**
     * Get an array of column orders so that added columns can be slotted into their correct spot.
     *
     * @param array $cdefs An array of column definitions.
     * @return array Returns an array of column orders suitable for an `alter table` statement.
     */
    protected function getColumnOrders($cdefs) {
        $orders = array_flip(array_keys($cdefs));

        $prev = ' first';
        foreach ($orders as $cname => &$value) {
            $value = $prev;
            $prev = ' after '.$this->escape($cname);
        }
        return $orders;
    }

    /**
     * Force a value into the appropriate php type based on its SQL type.
     *
     * @param mixed $value The value to force.
     * @param string $type The sqlite type name.
     * @return mixed Returns $value cast to the appropriate type.
     */
    protected function forceType($value, $type) {
        $type = strtolower($type);

        if ($type === 'null') {
            return null;
        } elseif (in_array($type, ['int', 'integer', 'tinyint', 'smallint',
            'mediumint', 'bigint', 'unsigned big int', 'int2', 'int8', 'boolean'])) {
            return force_int($value);
        } elseif (in_array($type, ['real', 'double', 'double precision', 'float',
            'numeric', 'decimal(10,5)'])) {
            return floatval($value);
        } else {
            return (string)$value;
        }
    }
}
