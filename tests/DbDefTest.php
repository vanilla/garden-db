<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Db;
use Garden\Db\TableDef;

/**
 * Test various aspects of the {@link TableDef} class and the {@link Db} class as it relates to it.
 */
abstract class DbDefTest extends AbstractDbTest {
    protected static function getPx() {
        return 'dbdef_';
    }

    /**
     * Test a basic call to {@link Db::createTable()}.
     */
    public function testCreateTable() {
        $def = static::createDbDef();
        $db = self::$db;

        $def1 = $def->table('user')
            ->primaryKey('userID')
            ->column('name', 'varchar(50)')
            ->index(Db::INDEX_IX, 'name')
            ->toArray();

        $db->defineTable($def->toArray());
        $def2 = $db->getTableDef('user');

        $this->assertDefEquals($def1, $def2);
    }

    /**
     * Test altering a table's columns.
     */
    public function testAlterTableColumns() {
        $db = self::$db;
        $def = new TableDef();
        $tbl = 'tstAlterTableColumns';

        $def->table($tbl)
            ->column('col1', 'int', false)
            ->column('col2', 'int', 0)
            ->index(Db::INDEX_IX, 'col1');

        $expected = $def->table($tbl)
            ->column('cola', 'int', false)
            ->column('colb', 'int', false)
            ->column('col2', 'int', false)
            ->index(Db::INDEX_IX, 'col1')
            ->toArray();

        $db->defineTable($expected);
        $db->reset();
        $actual = $db->getTableDef($tbl);

        $this->assertDefEquals($expected, $actual);
    }

    /**
     * Test altering a table with the {@link Db::OPTION_DROP} option.
     */
    public function testAlterTableWithDrop() {
        $db = self::$db;
        $def = new TableDef($db);
        $tbl = 'tstAlterTableWithDrop';

        $def->table($tbl)
            ->column('col1', 'int')
            ->column('col2', 'int', 0)
            ->index(Db::INDEX_IX, 'col1');
        $db->defineTable($def->toArray());

        $expected = $def->table($tbl)
            ->column('cola', 'int')
            ->column('colb', 'int')
            ->column('col2', 'int')
            ->index(Db::INDEX_IX, 'col2')
            ->toArray();
        $db->defineTable($expected, [Db::OPTION_DROP => true]);

        $actual = $db
            ->reset()
            ->getTableDef($tbl);

        $this->assertDefEquals($expected, $actual, false);
    }

    /**
     * Test altering the primary key.
     */
    public function testAlterPrimaryKey() {
        $db = self::$db;
        $def = new TableDef($db);
        $tbl = 'tstAlterPrimaryKey';

        $def->table($tbl)
            ->column('col1', 'int')
            ->column('col2', 'int', 0)
            ->index(Db::INDEX_PK, 'col1');
        $db->defineTable($def->toArray());

        $def->table($tbl)
            ->column('col1', 'int')
            ->column('col2', 'int', 0)
            ->index(Db::INDEX_PK, 'col1', 'col2');
        $db->defineTable($def->toArray());

        $expected = $db->getTableDef($tbl);

        $db->reset();
        $actual = $db->getTableDef($tbl);

        $this->assertDefEquals($expected, $actual);
    }

    /**
     * A more real world example of altering a primary key is re-ordering the primary key.
     */
    public function testReorderPrimaryKey() {
        $db = self::$db;
        $def = new TableDef($db);
        $tbl = 'tstReorderPrimaryKey';

        $def->table($tbl)
            ->column('col1', 'int')
            ->column('col2', 'int', 0)
            ->index(Db::INDEX_PK, 'col1', 'col2');
        $db->defineTable($def->toArray());

        $def->table($tbl)
            ->column('col1', 'int')
            ->column('col2', 'int', 0)
            ->index(Db::INDEX_PK, 'col2', 'col1');
        $db->defineTable($def->toArray());

        $expected = $db->getTableDef($tbl);

        $actual = $db
            ->reset()
            ->getTableDef($tbl);

        $this->assertDefEquals($expected, $actual);

        $db->reset();
        $actual =  $db->getTableDef($tbl);

        $this->assertDefEquals($expected, $actual);
    }
}
