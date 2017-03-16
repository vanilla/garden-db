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
 * A {@link Db} class for connecting to SQLite.
 */
class SqliteDb extends MySqlDb {
    /**
     * {@inheritdoc}
     */
    protected function alterTableDb(array $alterDef, array $options = []) {
        $this->alterTableMigrate($alterDef, $options);
    }

    /**
     * Alter a table by creating a new table and copying the old table's data to it.
     *
     * @param array $alterDef The new definition.
     * @param array $options An array of options for the migration.
     */
    private function alterTableMigrate(array $alterDef, array $options = []) {
        $table = $alterDef['name'];
        $currentDef = $this->fetchTableDef($table);

        // Merge the table definitions if we aren't dropping stuff.
        if (!self::val(Db::OPTION_DROP, $options)) {
            $tableDef = $this->mergeTableDefs($currentDef, $alterDef);
        } else {
            $tableDef = $alterDef['def'];
        }

        // Drop all of the indexes on the current table.
        foreach (self::val('indexes', $currentDef, []) as $indexDef) {
            if (self::val('type', $indexDef, Db::INDEX_IX) === Db::INDEX_IX) {
                $this->dropIndex($indexDef['name']);
            }
        }

        $tmpTable = $table.'_'.time();

        // Rename the current table.
        $this->renameTable($table, $tmpTable);

        // Create the new table.
        $this->createTableDb($tableDef, $options);

        // Figure out the columns that we can insert.
        $columns = array_keys(array_intersect_key($tableDef['columns'], $currentDef['columns']));

        // Build the insert/select statement.
        $sql = 'insert into '.$this->prefixTable($table)."\n".
            $this->bracketList($columns, '`')."\n".
            $this->buildSelect($tmpTable, [], ['columns' => $columns]);

        $this->queryDefine($sql);

        // Drop the temp table.
        $this->dropTable($tmpTable);
    }

    /**
     * Rename a table.
     *
     * @param string $old The old name of the table.
     * @param string $new The new name of the table.
     */
    private function renameTable($old, $new) {
        $renameSql = 'alter table '.
            $this->prefixTable($old).
            ' rename to '.
            $this->prefixTable($new);
        $this->queryDefine($renameSql);
    }

    /**
     * Merge a table def with its alter def so that no columns/indexes are lost in an alter.
     *
     * @param array $tableDef The table def.
     * @param array $alterDef The alter def.
     * @return array The new table def.
     */
    private function mergeTableDefs(array $tableDef, array $alterDef) {
        $result = $tableDef;

        if ($this->findPrimaryKeyIndex($alterDef['add']['indexes'])) {
            $remove = null;
            foreach ($result['indexes'] as $i => $index) {
                if ($index['type'] === Db::INDEX_PK) {
                    $remove = $i;
                }
            }
            if ($remove !== null) {
                unset($result['indexes'][$i]);
            }
        }

        $result['columns'] = array_merge($result['columns'], $alterDef['def']['columns']);
        $result['indexes'] = array_merge($result['indexes'], $alterDef['add']['indexes']);

        return $result;
    }

    /**
     * Drop an index.
     *
     * @param string $index The name of the index to drop.
     */
    protected function dropIndex($index) {
        $sql = 'drop index if exists '.
            $this->escape($index);
        $this->queryDefine($sql);
    }

    /**
     * {@inheritdoc}
     */
    protected function buildInsert($table, array $row, $options = []) {
        if (self::val(Db::OPTION_UPSERT, $options)) {
            throw new \Exception("Upsert is not supported.");
        } elseif (self::val(Db::OPTION_IGNORE, $options)) {
            $sql = 'insert or ignore into ';
        } elseif (self::val(Db::OPTION_REPLACE, $options)) {
            $sql = 'insert or replace into ';
        } else {
            $sql = 'insert into ';
        }
        $sql .= $this->prefixTable($table);

        // Add the list of values.
        $sql .=
            "\n".$this->bracketList(array_keys($row), '`').
            "\nvalues".$this->bracketList($row, "'");

        return $sql;
    }

    /**
     * {@inheritdoc}
     */
    protected function buildLike($column, $value) {
        return "$column like ".$this->quote($value)." escape '\\'";
    }

    /**
     * {@inheritdoc}
     */
    protected function buildUpdate($table, array $set, array $where, array $options = []) {
        $sql = 'update '.
            (empty($options[Db::OPTION_IGNORE]) ? '' : 'or ignore ').
            $this->prefixTable($table).
            "\nset\n  ";

        $parts = [];
        foreach ($set as $key => $value) {
            $escapedKey = $this->escape($key);
            $parts[] = "$escapedKey = ".$this->quote($value, $escapedKey);
        }
        $sql .= implode(",\n  ", $parts);

        if (!empty($where)) {
            $sql .= "\nwhere ".$this->buildWhere($where, Db::OP_AND);
        }

        return $sql;
    }

    /**
     * Construct a column definition string from an array defintion.
     *
     * @param string $name The name of the column.
     * @param array $cdef The column definition.
     * @return string Returns a string representing the column definition.
     */
    protected function columnDefString($name, array $cdef) {
        $cdef += [
            'autoIncrement' => false,
            'primary' => false,
            'allowNull' => false
        ];

        // Auto-increments MUST be of type integer.
        if ($cdef['autoIncrement']) {
            $cdef['dbtype'] = 'integer';
        }

        $result = $this->escape($name).' '.$this->nativeDbType($cdef);

        if ($cdef['primary'] && $cdef['autoIncrement']) {
//            if (val('autoincrement', $def)) {
                $result .= ' primary key autoincrement';
                $cdef['primary'] = true;
//            }
        } else {
            if (!$cdef['allowNull']) {
                $result .= ' not null';
            }

            if (isset($cdef['default'])) {
                $result .= ' default '.$this->quote($cdef['default']);
            }
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    protected function nativeDbType(array $type) {
        static $translations = ['bool' => 'boolean', 'byte' => 'tinyint', 'short' => 'smallint', 'long' => 'bigint'];

        // Translate the dbtype to a MySQL native type.
        if (isset($translations[$type['dbtype']])) {
            $type['dbtype'] = $translations[$type['dbtype']];
        }

        if (!empty($type['autoIncrement'])) {
            $type['dbtype'] = 'integer';
        }

        // Unsigned is represented differently in MySQL.
        $unsigned = !empty($type['unsigned']) && empty($type['autoIncrement']);
        unset($type['unsigned']);

        $dbType = static::dbType($type).($unsigned ? ' unsigned' : '');

        return $dbType;
    }

    /**
     * {@inheritdoc}
     */
    protected function createTableDb(array $tableDef, array $options = []) {
        $table = $tableDef['name'];
        $parts = [];

        // Make sure the primary key columns are defined first and in order.
        $autoInc = false;
        if ($pkIndex = $this->findPrimaryKeyIndex($tableDef['indexes'])) {
            foreach ($pkIndex['columns'] as $column) {
                $cdef = $tableDef['columns'][$column];
                $parts[] = $this->columnDefString($column, $cdef);
                $autoInc |= !empty($cdef['autoIncrement']);
                unset($tableDef['columns'][$column]);
            }
        }

        foreach ($tableDef['columns'] as $name => $cdef) {
            $parts[] = $this->columnDefString($name, $cdef);
        }

        // Add the primary key index.
        if (isset($pkIndex) && !$autoInc) {
            $parts[] = 'primary key '.$this->bracketList($pkIndex['columns'], '`');
        }

        $fullTableName = $this->prefixTable($table);
        $sql = "create table $fullTableName (\n  ".
            implode(",\n  ", $parts).
            "\n)";

        $this->queryDefine($sql);

        // Add the rest of the indexes.
        foreach (self::val('indexes', $tableDef, []) as $index) {
            if (self::val('type', $index, Db::INDEX_IX) !== Db::INDEX_PK) {
                $this->createIndex($table, $index, $options);
            }
        }
    }

    /**
     * Create an index.
     *
     * @param string $table The name of the table to create the index on.
     * @param array $indexDef The index definition.
     * @param array $options Additional options for the index creation.
     */
    public function createIndex($table, array $indexDef, $options = []) {
        $sql = 'create '.
            (self::val('type', $indexDef) === Db::INDEX_UNIQUE ? 'unique ' : '').
            'index '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'if not exists ' : '').
            $this->buildIndexName($table, $indexDef).
            ' on '.
            $this->prefixTable($table).
            $this->bracketList($indexDef['columns'], '`');

        $this->queryDefine($sql);
    }

    /**
     * Force a value into the appropriate php type based on its Sqlite type.
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
            return (int)filter_var($value, FILTER_VALIDATE_INT);
        } elseif (in_array($type, ['real', 'double', 'double precision', 'float',
            'numeric', 'decimal(10,5)'])) {
            return filter_var($value, FILTER_VALIDATE_FLOAT);
        } else {
            return (string)$value;
        }
    }

    /**
     * Get the columns for a table..
     *
     * @param string $table The table to get the columns for.
     * @return array|null Returns an array of columns.
     */
    protected function fetchColumnDefsDb($table) {
        $cdefs = $this->query('pragma table_info('.$this->prefixTable($table, false).')')->fetchAll(PDO::FETCH_ASSOC);
        if (empty($cdefs)) {
            return null;
        }

        $columns = [];
        $pk = [];
        foreach ($cdefs as $cdef) {
            $column = Db::typeDef($cdef['type']);
            $column = Db::typeDef($cdef['type']);
            if ($column === null) {
                throw new \Exception("Unknown type '$columnType'.", 500);
            }
            $column['allowNull'] = !filter_var($cdef['notnull'], FILTER_VALIDATE_BOOLEAN);

            if ($cdef['pk']) {
                $pk[] = $cdef['name'];
                if (strcasecmp($cdef['type'], 'integer') === 0) {
                    $column['autoIncrement'] = true;
                } else {
                    $column['primary'] = true;
                }
            }
            if ($cdef['dflt_value'] !== null) {
                $column['default'] = $this->forceType($cdef['dflt_value'], $column['type']);
            }
            $columns[$cdef['name']] = $column;
        }
//        $tdef = ['columns' => $columns];
//        if (!empty($pk)) {
//            $tdef['indexes'][Db::INDEX_PK] = [
//                'columns' => $pk,
//                'type' => Db::INDEX_PK
//            ];
//        }
//        $this->tables[$table] = $tdef;
        return $columns;
    }

    /**
     * Get the indexes for a table.
     *
     * @param string $table The name of the table to get the indexes for or an empty string to get all indexes.
     * @return array|null
     */
    protected function fetchIndexesDb($table = '') {
        $indexes = [];

        $indexInfos = $this->query('pragma index_list('.$this->prefixTable($table).')')->fetchAll(PDO::FETCH_ASSOC);
        foreach ($indexInfos as $row) {
            $indexName = $row['name'];
            if ($row['unique']) {
                $type = Db::INDEX_UNIQUE;
            } else {
                $type = Db::INDEX_IX;
            }

            // Query the columns in the index.
            $columns = $this->query('pragma index_info('.$this->quote($indexName).')')->fetchAll(PDO::FETCH_ASSOC);

            $index = [
                'name' => $indexName,
                'columns' => array_column($columns, 'name'),
                'type' => $type
            ];
            $indexes[] = $index;
        }

        return $indexes;
    }

    /**
     * Get the primary or secondary keys from the given rows.
     *
     * @param string $table The name of the table.
     * @param array $row The row to examine.
     * @param bool $quick Whether or not to quickly look for <tablename>ID for the primary key.
     * @return array|null Returns the primary keys and values from {@link $rows} or null if the primary key isn't found.
     */
    private function getPKValue($table, array $row, $quick = false) {
        if ($quick && isset($row[$table.'ID'])) {
            return [$table.'ID' => $row[$table.'ID']];
        }

        $tdef = $this->fetchTableDef($table);
        $cols = [];
        foreach ($tdef['columns'] as $name => $cdef) {
            if (empty($cdef['primary'])) {
                break;
            }
            if (!array_key_exists($name, $row)) {
                return null;
            }

            $cols[$name] = $row[$name];
        }
        return $cols;
    }

    /**
     * Get the all of table names in the database.
     *
     * @return array Returns an array of table names.
     */
    protected function fetchTableNamesDb() {
        // Get the table names.
        $tables = $this->get(
            new Identifier('sqlite_master'),
            [
                'type' => 'table',
                'name' => [Db::OP_LIKE => $this->escapeLike($this->getPx()).'%']
            ],
            [
                'columns' => ['name']
            ]
        )->fetchAll(PDO::FETCH_COLUMN);

        // Remove internal tables.
        $tables = array_filter($tables, function ($name) {
            return substr($name, 0, 7) !== 'sqlite_';
        });

        return $tables;
    }

    /**
     * {@inheritdoc}
     */
    public function insert($table, array $row, array $options = []) {
        // Sqlite doesn't support upsert so do upserts manually.
        if (self::val(Db::OPTION_UPSERT, $options)) {
            unset($options[Db::OPTION_UPSERT]);

            $keys = $this->getPKValue($table, $row, true);
            if (empty($keys)) {
                throw new \Exception("Cannot upsert with no key.", 500);
            }
            // Try updating first.
            $updated = $this->update(
                $table,
                array_diff_key($row, $keys),
                $keys,
                $options
            );
            if ($updated) {
                // Updated.
                if (count($keys) === 1) {
                    return array_pop($keys);
                } else {
                    return true;
                }
            }
        }

        $result = parent::insert($table, $row, $options);
        return $result;
    }

    /**
     * Optionally quote a where value.
     *
     * @param mixed $value The value to quote.
     * @param string $column The name of the column being operated on.
     * @return string Returns the value, optionally quoted.
     * @internal param bool $quote Whether or not to quote the value.
     */
    public function quote($value, $column = '') {
        if ($value instanceof Literal) {
            /* @var Literal $value */
            return $value->getValue($this, $column);
        } elseif (in_array(gettype($value), ['integer', 'double'])) {
            return (string)$value;
        } elseif ($value instanceof \DateTimeInterface) {
            $value = $value->format(\DateTime::RFC3339);
        } elseif ($value === true) {
            return '1';
        } elseif ($value === false) {
            return '0';
        }

        return $this->getPDO()->quote($value);
    }
}
