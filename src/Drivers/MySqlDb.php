<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Drivers;

use Garden\Db\Db;
use Garden\Db\Identifier;
use Garden\Db\Literal;
use PDO;

/**
 * A {@link Db} class for connecting to MySQL.
 */
class MySqlDb extends Db {
    const MYSQL_DATE_FORMAT = 'Y-m-d H:i:s';

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
    ];

    /**
     * {@inheritdoc}
     */
    protected function dropTableDb($table, array $options = []) {
        $sql = 'drop table '.
            ($options[Db::OPTION_IGNORE] ? 'if exists ' : '').
            $this->prefixTable($table);

        $this->queryDefine($sql);
    }

    /**
     * {@inheritdoc}
     */
    protected function fetchTableDefDb($table) {
        $columns = $this->fetchColumnDefsDb($table);

        if (empty($columns)) {
            // A table with no columns does not exist.
            return null;
        }

        $indexes = $this->fetchIndexesDb($table);

        $tableDef = [
            'name' => $table,
            'columns' => $columns,
            'indexes' => $indexes
        ];

        return $tableDef;
    }

    /**
     * {@inheritdoc}
     */
    protected function fetchColumnDefsDb($table) {
        $rows = $this->get(
            new Identifier('information_schema', 'COLUMNS'),
            [
                'TABLE_SCHEMA' => $this->getDbName(),
                'TABLE_NAME' => $this->prefixTable($table, false)
            ],
            [
                Db::OPTION_FETCH_MODE => PDO::FETCH_ASSOC,
                'order' => ['TABLE_NAME', 'ORDINAL_POSITION']
            ]
        );

        $columns = [];
        foreach ($rows as $row) {
            $columnType = $row['COLUMN_TYPE'];
            if ($columnType === 'tinyint(1)') {
                $columnType = 'bool';
            }
            $column = Db::typeDef($columnType);
            if ($column === null) {
                throw new \Exception("Unknown type '$columnType'.", 500);
            }

            $column['allowNull'] = strcasecmp($row['IS_NULLABLE'], 'YES') === 0;

            if (($default = $row['COLUMN_DEFAULT']) !== null) {
                $column['default'] = $this->forceType($default, $column['type']);
            }

            if ($row['EXTRA'] === 'auto_increment') {
                $column['autoIncrement'] = true;
            }

            if ($row['COLUMN_KEY'] === 'PRI') {
                $column['primary'] = true;
            }

            $columns[$row['COLUMN_NAME']] = $column;
        }

        return $columns;
    }

    /**
     * {@inheritdoc}
     */
    public function get($table, array $where, array $options = []) {
        $sql = $this->buildSelect($table, $where, $options);
        $result = $this->query($sql, [], $options);
        return $result;
    }

    /**
     * Build a sql select statement.
     *
     * @param string|Identifier $table The name of the main table.
     * @param array $where The where filter.
     * @param array $options An array of additional query options.
     * @return string Returns the select statement as a string.
     * @see Db::get()
     */
    protected function buildSelect($table, array $where, array $options = []) {
        $options += ['limit' => 0];

        $sql = '';

        // Build the select clause.
        if (!empty($options['columns'])) {
            $columns = array();
            foreach ($options['columns'] as $value) {
                $columns[] = $this->escape($value);
            }
            $sql .= 'select '.implode(', ', $columns);
        } else {
            $sql .= "select *";
        }

        // Build the from clause.
        if ($table instanceof Literal) {
            $table = $table->getValue($this);
        } else {
            $table = $this->prefixTable($table);
        }
        $sql .= "\nfrom $table";

        // Build the where clause.
        $whereString = $this->buildWhere($where, Db::OP_AND);
        if ($whereString) {
            $sql .= "\nwhere ".$whereString;
        }

        // Build the order.
        if (!empty($options['order'])) {
            $orders = [];
            foreach ($options['order'] as $column) {
                if ($column[0] === '-') {
                    $order = $this->escape(substr($column, 1)).' desc';
                } else {
                    $order = $this->escape($column);
                }
                $orders[] = $order;
            }
            $sql .= "\norder by ".implode(', ', $orders);
        }

        // Build the limit, offset.
        if (!empty($options['limit'])) {
            $limit = (int)$options['limit'];
            $sql .= "\nlimit $limit";
        }

        if (!empty($options['offset'])) {
            $sql .= ' offset '.((int)$options['offset']);
        } elseif (isset($options['page'])) {
            $offset = $options['limit'] * ($options['page'] - 1);
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
     * {@inheritdoc}
     */
    protected function nativeDbType(array $type) {
        static $translations = ['bool' => 'tinyint(1)', 'byte' => 'tinyint', 'short' => 'smallint', 'long' => 'bigint'];

        // Translate the dbtype to a MySQL native type.
        if (isset($translations[$type['dbtype']])) {
            $type['dbtype'] = $translations[$type['dbtype']];
        }

        // Unsigned is represented differently in MySQL.
        $unsigned = !empty($type['unsigned']);
        unset ($type['unsigned']);

        $dbType = static::dbType($type).($unsigned ? ' unsigned' : '');

        return $dbType;
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
     * @param string $table The name of the table to get the indexes for.
     * @return array|null
     */
    protected function fetchIndexesDb($table = '') {
        $stm = $this->get(
            new Identifier('information_schema', 'STATISTICS'),
            [
                'TABLE_SCHEMA' => $this->getDbName(),
                'TABLE_NAME' => $this->prefixTable($table, false)
            ],
            [
                'columns' => [
                    'INDEX_NAME',
                    'COLUMN_NAME',
                    'NON_UNIQUE'
                ],
                'order' => ['INDEX_NAME', 'SEQ_IN_INDEX']
            ]
        );
        $indexRows = $stm->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

        $indexes = [];
        foreach ($indexRows as $indexName => $columns) {
            $index = [
                'type' => null,
                'columns' => array_column($columns, 'COLUMN_NAME'),
                'name' => $indexName
            ];

            if ($indexName === 'PRIMARY') {
                $index['type'] = Db::INDEX_PK;
            } else {
                $index['type'] = $columns[0]['NON_UNIQUE'] ? Db::INDEX_IX : Db::INDEX_UNIQUE;
            }
            $indexes[] = $index;
        }

        return $indexes;
    }

    /**
     * {@inheritdoc}
     */
    protected function fetchTableNamesDb() {
        // Get the table names.
        $tables = $this->get(
            new Identifier('information_schema', 'TABLES'),
            [
                'TABLE_SCHEMA' => $this->getDbName(),
                'TABLE_NAME' => [Db::OP_LIKE => $this->escapeLike($this->getPx()).'%']
            ],
            [
                'columns' => ['TABLE_NAME'],
                'fetchMode' => PDO::FETCH_ASSOC
            ]
        );

        return $tables->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * {@inheritdoc}
     */
    public function insert($table, array $row, array $options = []) {
        $sql = $this->buildInsert($table, $row, $options);
        $id = $this->queryID($sql, [], $options);
        if (is_numeric($id)) {
            return (int)$id;
        } else {
            return $id;
        }
    }

    /**
     * Build an insert statement.
     *
     * @param string|Identifier $table The name of the table to insert to.
     * @param array $row The row to insert.
     * @param array $options An array of options for the insert. See {@link Db::insert} for the options.
     * @return string Returns the the sql string of the insert statement.
     */
    protected function buildInsert($table, array $row, $options = []) {
        if (self::val(Db::OPTION_UPSERT, $options)) {
            return $this->buildUpsert($table, $row, $options);
        } elseif (self::val(Db::OPTION_IGNORE, $options)) {
            $sql = 'insert ignore ';
        } elseif (self::val(Db::OPTION_REPLACE, $options)) {
            $sql = 'replace ';
        } else {
            $sql = 'insert ';
        }
        $sql .= $this->prefixTable($table);

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
     * @param string $table The name of the table to update.
     * @param array $row The row to insert or update.
     * @param array $options An array of additional query options.
     * @return string Returns the upsert statement as a string.
     */
    protected function buildUpsert($table, array $row, $options = []) {
        // Build the initial insert statement first.
        unset($options[Db::OPTION_UPSERT]);
        $sql = $this->buildInsert($table, $row, $options);

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
    public function load($table, $rows, array $options = []) {
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

                $sql = $this->buildInsert($table, $spec, $options);
                $stmt = $this->getPDO()->prepare($sql);
                $first = false;
            }

            $stmt->execute($row);
            $count += $stmt->rowCount();
        }

        return $count;
    }

    /**
     * Make a valid PDO parameter name from a string.
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
    public function update($table, array $set, array $where, array $options = []) {
        $sql = $this->buildUpdate($table, $set, $where, $options);
        $result = $this->queryModify($sql, [], $options);

        return $result;
    }

    /**
     * Build a sql update statement.
     *
     * @param string|Identifier $table The name of the table to update.
     * @param array $set An array of columns to set.
     * @param array $where The where filter.
     * @param array $options Additional options for the query.
     * @return string Returns the update statement as a string.
     */
    protected function buildUpdate($table, array $set, array $where, array $options = []) {
        $sql = 'update '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'ignore ' : '').
            $this->prefixTable($table).
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
    public function delete($table, array $where, array $options = []) {
        if (self::val(Db::OPTION_TRUNCATE, $options)) {
            if (!empty($where)) {
                throw new \InvalidArgumentException("You cannot truncate $table with a where filter.", 500);
            }
            $sql = 'truncate table '.$this->prefixTable($table);
        } else {
            $sql = 'delete from '.$this->prefixTable($table);

            if (!empty($where)) {
                $sql .= "\nwhere ".$this->buildWhere($where);
            }
        }
        return $this->queryModify($sql, [], $options);
    }

    /**
     * {@inheritdoc}
     */
    protected function createTableDb(array $tableDef, array $options = []) {
        $table = $tableDef['name'];

        // The table doesn't exist so this is a create table.
        $parts = array();
        foreach ($tableDef['columns'] as $name => $cdef) {
            $parts[] = $this->columnDefString($name, $cdef);
        }

        foreach (self::val('indexes', $tableDef, []) as $index) {
            $indexDef = $this->indexDefString($table, $index);
            if (!empty($indexDef)) {
                $parts[] = $indexDef;
            }
        }

        $tableName = $this->prefixTable($table);
        $sql = "create table $tableName (\n  ".
            implode(",\n  ", $parts).
            "\n)";

        if (self::val('collate', $options)) {
            $sql .= "\n collate {$options['collate']}";
        }

        $this->queryDefine($sql, $options);
    }

    /**
     * Construct a column definition string from an array defintion.
     *
     * @param string $name The name of the column.
     * @param array $cdef The column definition.
     * @return string Returns a string representing the column definition.
     */
    protected function columnDefString($name, array $cdef) {
        $result = $this->escape($name).' '.$this->nativeDbType($cdef);

        if (!self::val('allowNull', $cdef)) {
            $result .= ' not null';
        }

        if (isset($cdef['default'])) {
            $result .= ' default '.$this->quote($cdef['default']);
        }

        if (self::val('autoIncrement', $cdef)) {
            $result .= ' auto_increment';
        }

        return $result;
    }

    /**
     * Return the SDL string that defines an index.
     *
     * @param string $table The name of the table that the index is on.
     * @param array $def The index definition. This definition should have the following keys.
     *
     * columns
     * : An array of columns in the index.
     * type
     * : One of "index", "unique", or "primary".
     * @return null|string Returns the index string or null if the index is not correct.
     */
    protected function indexDefString($table, array $def) {
        $indexName = $this->escape($this->buildIndexName($table, $def));
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
    protected function alterTableDb(array $alterDef, array $options = []) {
        $table = $alterDef['name'];
        $columnOrders = $this->getColumnOrders($alterDef['def']['columns']);
        $parts = [];

        // Add the columns and indexes.
        foreach ($alterDef['add']['columns'] as $cname => $cdef) {
            // Figure out the order of the column.
            $pos = self::val($cname, $columnOrders, '');
            $parts[] = 'add '.$this->columnDefString($cname, $cdef).$pos;
        }
        foreach ($alterDef['add']['indexes'] as $ixdef) {
            $parts[] = 'add '.$this->indexDefString($table, $ixdef);
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
            'table '.$this->prefixTable($table)."\n  ".
            implode(",\n  ", $parts);

        $this->queryDefine($sql, $options);
    }

    /**
     * Get an array of column orders so that added columns can be slotted into their correct spot.
     *
     * @param array $cdefs An array of column definitions.
     * @return array Returns an array of column orders suitable for an `alter table` statement.
     */
    private function getColumnOrders($cdefs) {
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
        } elseif ($type === 'boolean') {
            return filter_var($value, FILTER_VALIDATE_BOOLEAN);
        } elseif (in_array($type, ['int', 'integer', 'tinyint', 'smallint',
            'mediumint', 'bigint', 'unsigned big int', 'int2', 'int8', 'boolean'])) {
            return filter_var($value, FILTER_VALIDATE_INT);
        } elseif (in_array($type, ['real', 'double', 'double precision', 'float',
            'numeric', 'number', 'decimal(10,5)'])) {
            return filter_var($value, FILTER_VALIDATE_FLOAT);
        } else {
            return (string)$value;
        }
    }

    public function quote($value, $column = '') {
        if (is_bool($value)) {
            return (string)(int)$value;
        } else {
            return parent::quote($value, $column);
        }
    }
}
