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
        $def = new TableDef('user');
        $db = self::$db;

        $def1 = $def
            ->setPrimaryKey('userID', 'uint')
            ->setColumn('name', 'varchar(50)')
            ->addIndex(Db::INDEX_IX, 'name')
            ->toArray();

        $db->defineTable($def->toArray());
        $def2 = $db->fetchTableDef('user');

        $this->assertDefEquals($def1, $def2);
    }

    /**
     * Test altering a table's columns.
     */
    public function testAlterTableColumns() {
        $db = self::$db;
        $def = new TableDef();
        $tbl = 'tstAlterTableColumns';

        $def->setTable($tbl)
            ->setColumn('col1', 'int', false)
            ->setColumn('col2', 'uint', 0)
            ->addIndex(Db::INDEX_IX, 'col1');
        $def->exec($db);

        $expected = $def
            ->setColumn('cola', 'int', false)
            ->setColumn('colb', 'bool', false)
            ->setColumn('col2', 'uint', false)
            ->toArray();

        $db->defineTable($expected);
        $db->reset();
        $actual = $db->fetchTableDef($tbl);

        $this->assertDefEquals($expected, $actual);
    }

    /**
     * Test altering a table with the {@link Db::OPTION_DROP} option.
     */
    public function testAlterTableWithDrop() {
        $db = self::$db;
        $def = new TableDef($db);
        $tbl = 'tstAlterTableWithDrop';

        $def->setTable($tbl)
            ->setColumn('col1', 'int')
            ->setColumn('col2', 'int', 0)
            ->addIndex(Db::INDEX_IX, 'col1');
        $db->defineTable($def->toArray());

        $expected = $def->setTable($tbl)
            ->setColumn('cola', 'int')
            ->setColumn('colb', 'int')
            ->setColumn('col2', 'int')
            ->addIndex(Db::INDEX_IX, 'col2')
            ->toArray();
        $db->defineTable($expected, [Db::OPTION_DROP => true]);

        $actual = $db
            ->reset()
            ->fetchTableDef($tbl);

        $this->assertDefEquals($expected, $actual, false);
    }

    /**
     * Test altering the primary key.
     */
    public function testAlterPrimaryKey() {
        $db = self::$db;
        $tbl = 'tstAlterPrimaryKey';
        $def = new TableDef($tbl);

        $def->setColumn('col1', 'int')
            ->setColumn('col2', 'int', 0)
            ->addIndex(Db::INDEX_PK, 'col1')
            ->exec($db);

        $def->addIndex(Db::INDEX_PK, 'col1', 'col2')
            ->exec($db);

        $expected = $db->fetchTableDef($tbl);

        $db->reset();
        $actual = $db->fetchTableDef($tbl);

        $this->assertDefEquals($expected, $actual);
    }

    /**
     * A more real world example of altering a primary key is re-ordering the primary key.
     */
    public function testReorderPrimaryKey() {
        $db = self::$db;
        $def = new TableDef($db);
        $tbl = 'tstReorderPrimaryKey';

        $def->setTable($tbl)
            ->setColumn('col1', 'int')
            ->setColumn('col2', 'int', 0)
            ->addIndex(Db::INDEX_PK, 'col1', 'col2');
        $db->defineTable($def->toArray());

        $def->setTable($tbl)
            ->setColumn('col1', 'int')
            ->setColumn('col2', 'int', 0)
            ->addIndex(Db::INDEX_PK, 'col2', 'col1');
        $db->defineTable($def->toArray());

        $expected = $db->fetchTableDef($tbl);

        $actual = $db
            ->reset()
            ->fetchTableDef($tbl);

        $this->assertDefEquals($expected, $actual);

        $db->reset();
        $actual =  $db->fetchTableDef($tbl);

        $this->assertDefEquals($expected, $actual);
    }
}
