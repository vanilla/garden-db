<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use PDO;

/**
 * A {@link Db} class for connecting to SQLite.
 */
class SqliteDb extends MySqlDb {
    /**
     * {@inheritdoc}
     */
    protected function alterTable(array $alterDef, array $options = []) {
        $tablename = $alterDef['name'];
        $this->alterTableMigrate($tablename, $alterDef, $options);
    }

    /**
     * Alter a table by creating a new table and copying the old table's data to it.
     *
     * @param string $tablename The table to alter.
     * @param array $alterDef The new definition.
     * @param array $options An array of options for the migration.
     */
    private function alterTableMigrate($tablename, array $alterDef, array $options = []) {
        $currentDef = $this->getTableDef($tablename);

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

        $tmpTablename = $tablename.'_'.time();

        // Rename the current table.
        $this->renameTable($tablename, $tmpTablename);

        // Create the new table.
        $this->createTable($tableDef, $options);

        // Figure out the columns that we can insert.
        $columns = array_keys(array_intersect_key($tableDef['columns'], $currentDef['columns']));

        // Build the insert/select statement.
        $sql = 'insert into '.$this->prefixTable($tablename)."\n".
            $this->bracketList($columns, '`')."\n".
            $this->buildSelect($tmpTablename, [], ['columns' => $columns]);

        $this->query($sql, Db::QUERY_WRITE);

        // Drop the temp table.
        $this->dropTable($tmpTablename);
    }

    /**
     * Rename a table.
     *
     * @param string $oldname The old name of the table.
     * @param string $newname The new name of the table.
     */
    private function renameTable($oldname, $newname) {
        $renameSql = 'alter table '.
            $this->prefixTable($oldname).
            ' rename to '.
            $this->prefixTable($newname);
        $this->query($renameSql, Db::QUERY_WRITE);
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

        $result['columns'] = array_merge($result['columns'], $alterDef['def']['columns']);
        $result['indexes'] = array_merge($result['indexes'], $alterDef['add']['indexes']);

        return $result;
    }

    /**
     * Drop an index.
     *
     * @param string $indexName The name of the index to drop.
     */
    protected function dropIndex($indexName) {
        $sql = 'drop index if exists '.
            $this->escape($indexName);
        $this->query($sql, Db::QUERY_DEFINE);
    }

    /**
     * {@inheritdoc}
     */
    protected function buildInsert($tableName, array $row, $options = []) {
        if (self::val(Db::OPTION_UPSERT, $options)) {
            throw new \Exception("Upsert is not supported.");
        } elseif (self::val(Db::OPTION_IGNORE, $options)) {
            $sql = 'insert or ignore into ';
        } elseif (self::val(Db::OPTION_REPLACE, $options)) {
            $sql = 'insert or replace into ';
        } else {
            $sql = 'insert into ';
        }
        $sql .= $this->prefixTable($tableName);

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
    protected function buildUpdate($tableName, array $set, array $where, array $options = []) {
        $sql = 'update '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'or ignore ' : '').
            $this->prefixTable($tableName).
            "\nset\n  ";

        $parts = [];
        foreach ($set as $key => $value) {
            $parts[] = $this->escape($key).' = '.$this->quote($value);
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
     * @param array $def The column definition.
     * @return string Returns a string representing the column definition.
     */
    protected function columnDefString($name, array $def) {
        $def += [
            'autoIncrement' => false,
            'primary' => false,
            'allowNull' => false
        ];

        // Auto-increments MUST be of type integer.
        if ($def['autoIncrement']) {
            $def['dbtype'] = 'integer';
        }

        $result = $this->escape($name).' '.$this->columnTypeString($def['dbtype']);

        if ($def['primary'] && $def['autoIncrement']) {
//            if (val('autoincrement', $def)) {
                $result .= ' primary key autoincrement';
                $def['primary'] = true;
//            }
        } elseif (isset($def['default'])) {
            $result .= ' default '.$this->quote($def['default']);
        } elseif (!$def['allowNull']) {
            $result .= ' not null';
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    protected function createTable(array $tableDef, array $options = []) {
        $tablename = $tableDef['name'];
        $parts = [];

        // Make sure the primary key columns are defined first and in order.
        $autoinc = false;
        if (isset($tableDef['indexes']['primary'])) {
            $pkIndex = $tableDef['indexes']['primary'];
            foreach ($pkIndex['columns'] as $column) {
                $cdef = $tableDef['columns'][$column];
                $parts[] = $this->columnDefString($column, $cdef);
                $autoinc |= self::val('autoIncrement', $cdef, false);
                unset($tableDef['columns'][$column]);
            }
        }

        foreach ($tableDef['columns'] as $name => $cdef) {
            $parts[] = $this->columnDefString($name, $cdef);
        }

        // Add the prinary key index.
        if (isset($pkIndex) && !$autoinc) {
            $parts[] = 'primary key '.$this->bracketList($pkIndex['columns'], '`');
        }

        $fullTablename = $this->prefixTable($tablename);
        $sql = "create table $fullTablename (\n  ".
            implode(",\n  ", $parts).
            "\n)";

        $this->query($sql, Db::QUERY_DEFINE);

        // Add the rest of the indexes.
        foreach (self::val('indexes', $tableDef, []) as $index) {
            if (self::val('type', $index, Db::INDEX_IX) !== Db::INDEX_PK) {
                $this->createIndex($tablename, $index, $options);
            }
        }
    }

    /**
     * Create an index.
     *
     * @param string $tablename The name of the table to create the index on.
     * @param array $indexDef The index definition.
     * @param array $options Additional options for the index creation.
     */
    public function createIndex($tablename, array $indexDef, $options = []) {
        $sql = 'create '.
            (self::val('type', $indexDef) === Db::INDEX_UNIQUE ? 'unique ' : '').
            'index '.
            (self::val(Db::OPTION_IGNORE, $options) ? 'if not exists ' : '').
            $this->buildIndexName($tablename, $indexDef).
            ' on '.
            $this->prefixTable($tablename).
            $this->bracketList($indexDef['columns'], '`');

        $this->query($sql, Db::QUERY_DEFINE);
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
            return force_int($value);
        } elseif (in_array($type, ['real', 'double', 'double precision', 'float',
            'numeric', 'decimal(10,5)'])) {
            return floatval($value);
        } else {
            return (string)$value;
        }
    }

    /**
     * Get the columns for tables and put them in {MySqlDb::$tables}.
     *
     * @param string $tableName The table to get the columns for or blank for all columns.
     * @return array|null Returns an array of columns if {@link $tablename} is specified, or null otherwise.
     */
    protected function getColumns($tableName = '') {
        if (!$tableName) {
            $tablenames = $this->getTableNames();
            foreach ($tablenames as $tableName) {
                $this->getColumns($tableName);
            }
        }

        $cdefs = (array)$this->query('pragma table_info('.$this->quote($this->getPx().$tableName).')');
        if (empty($cdefs)) {
            return null;
        }

        $columns = [];
        $pk = [];
        foreach ($cdefs as $cdef) {
            $column = [
                'dbtype' => $this->columnTypeString($cdef['type']),
                'allowNull' => !force_bool($cdef['notnull'])//, FILTER_VALIDATE_BOOLEAN),
            ];
            if ($cdef['pk']) {
                $pk[] = $cdef['name'];
                if (strcasecmp($cdef['type'], 'integer') === 0) {
                    $column['autoincrement'] = true;
                } else {
                    $column['primary'] = true;
                }
            }
            if ($cdef['dflt_value'] !== null) {
                $column['default'] = $cdef['dflt_value'];
            }
            $columns[$cdef['name']] = $column;
        }
        $tdef = ['columns' => $columns];
        if (!empty($pk)) {
            $tdef['indexes'][Db::INDEX_PK] = [
                'columns' => $pk,
                'type' => Db::INDEX_PK
            ];
        }
        $this->tables[$tableName] = $tdef;
        return $columns;
    }

    /**
     * Get the indexes from the database.
     *
     * @param string $tableName The name of the table to get the indexes for or an empty string to get all indexes.
     * @return array|null
     */
    protected function getIndexes($tableName = '') {
        if (!$tableName) {
            $tablenames = $this->getTableNames();
            foreach ($tablenames as $tableName) {
                $this->getIndexes($tableName);
            }
        }

        $pk = valr(['indexes', Db::INDEX_PK], $this->tables[$tableName]);

        // Reset the index list for the table.
        $this->tables[$tableName]['indexes'] = [];

        if ($pk) {
            $this->tables[$tableName]['indexes'][Db::INDEX_PK] = $pk;
        }

        $indexInfos = (array)$this->query('pragma index_list('.$this->prefixTable($tableName).')');
        foreach ($indexInfos as $row) {
            $indexName = $row['name'];
            if ($row['unique']) {
                $type = Db::INDEX_UNIQUE;
            } else {
                $type = Db::INDEX_IX;
            }

            // Query the columns in the index.
            $columns = (array)$this->query('pragma index_info('.$this->quote($indexName).')');

            $index = [
                'name' => $indexName,
                'columns' => array_column($columns, 'name'),
                'type' => $type
            ];
            $this->tables[$tableName]['indexes'][] = $index;
        }

        return $this->tables[$tableName]['indexes'];
    }

    /**
     * Get the primary or secondary keys from the given rows.
     *
     * @param string $tablename The name of the table.
     * @param array $row The row to examine.
     * @param bool $quick Whether or not to quickly look for <tablename>ID for the primary key.
     * @return array|null Returns the primary keys and values from {@link $rows} or null if the primary key isn't found.
     */
    protected function getPKValue($tablename, array $row, $quick = false) {
        if ($quick && isset($row[$tablename.'ID'])) {
            return [$tablename.'ID' => $row[$tablename.'ID']];
        }

        $tdef = $this->getTableDef($tablename);
        if (isset($tdef['indexes'][Db::INDEX_PK]['columns'])) {
            $pkColumns = array_flip($tdef['indexes'][Db::INDEX_PK]['columns']);
            $cols = array_intersect_key($row, $pkColumns);
            if (count($cols) === count($pkColumns)) {
                return $cols;
            }
        }

        return null;
    }

    /**
     * Get the all of tablenames in the database.
     *
     * @return array Returns an array of table names with prefixes stripped.
     */
    protected function getTableNames() {
        // Get the table names.
        $tables = (array)$this->get(
            new Identifier('sqlite_master'),
            [
                'type' => 'table',
                'name' => [Db::OP_LIKE => $this->escapeLike($this->getPx()).'%']
            ],
            [
                'columns' => ['name']
            ]
        );
        $tables = array_column($tables, 'name');

        // Remove internal tables.
        $tables = array_filter($tables, function ($name) {
            return substr($name, 0, 7) !== 'sqlite_';
        });

        // Strip the table prefixes.
        $tables = array_map(function ($name) {
            return ltrim_substr($name, $this->getPx());
        }, $tables);

        return $tables;
    }

    /**
     * {@inheritdoc}
     */
    public function insert($tableName, array $rows, array $options = []) {
        // Sqlite doesn't support upsert so do upserts manually.
        if (self::val(Db::OPTION_UPSERT, $options)) {
            unset($options[Db::OPTION_UPSERT]);

            $keys = $this->getPKValue($tableName, $rows, true);
            if (!$keys) {
                throw new \Exception("Cannot upsert with no key.", 500);
            }
            // Try updating first.
            $updated = $this->update(
                $tableName,
                array_diff_key($rows, $keys),
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

        $result = parent::insert($tableName, $rows, $options);
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
        } elseif ($value === true) {
            return '1';
        } elseif ($value === false) {
            return '0';
        } else {
            return $this->getPDO()->quote($value);
        }
    }
}
