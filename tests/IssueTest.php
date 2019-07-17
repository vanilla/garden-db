<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2018 Vanilla Forums Inc.
 * @license Proprietary
 */

namespace Garden\Db\Tests;

use Garden\Db\Db;
use Garden\Db\TableDef;

abstract class IssueTest extends AbstractDbTest {
    protected static function getPx() {
        return 'issue_';
    }

    /**
     * This issue occurs when you alter a table against an existing database then the table name cache gets set for only the one table.
     *
     * This causes future database structure calls to fail.
     */
    public function testRedefineExisting() {
        $tb1 = new TableDef('a');
        $tb1->setColumn('foo', 'varchar(10)');

        $tb2 = new TableDef('b');
        $tb2->setColumn('foo', 'varchar(10)');

        $db = static::$db;
        $tb1->exec($db);
        $tb2->exec($db);
        $tables = $db->fetchTableNames();

        $db->reset();
        $tb1->setColumn('bar', 'varchar(10)');
        $tb1->exec($db);

        $b = $db->fetchTableDef('b');
        $this->assertNotNull($b);

//        $tb2->exec($db2);
    }

    /**
     * Test inserting a null value.
     */
    public function testNullInsert() {
        $tbl = new TableDef('null_insert');
        $tbl->setColumn('dt', 'datetime', null);

        $db = static::$db;
        $tbl->exec($db);

        $db->insert('null_insert', ['dt' => null]);

        // This test is just to check DB exceptions.
        $this->assertTrue(true);
    }

    /**
     * Test updating to a null value.
     */
    public function testNullUpdate() {
        $tbl = new TableDef('null_update');
        $tbl->setColumn('id', 'int')
            ->setColumn('dt', 'datetime', null)
            ->addIndex(Db::INDEX_PK, 'id');

        $db = static::$db;
        $tbl->exec($db);

        $db->insert('null_update', ['id' => 1, 'dt' => new \DateTime('2018-01-01')]);
        $r = $db->update('null_update', ['dt' => null], ['id' => 1]);

        // This test is just to check DB exceptions.
        $this->assertTrue(true);
    }

    /**
     * Test creating a column with medium text.
     */
    public function testTextCreate() {
        $tbl = new TableDef('text');
        $tbl->setPrimaryKey('id')
            ->setColumn('body0', 'tinytext')
            ->setColumn('body1', 'text')
            ->setColumn('body2', 'mediumtext')
            ->setColumn('body3', 'longtext');

        $tbl->exec(static::$db);

        $tbl->setPrimaryKey('id')
            ->setColumn('body0', 'longtext')
            ->setColumn('body1', 'mediumtext')
            ->setColumn('body2', 'text')
            ->setColumn('body3', 'tinytext');

        $tbl->exec(static::$db);

        // This test is just to check DB exceptions.
        $this->assertTrue(true);
    }

    /**
     * Test altering the length of a varchar column.
     */
    public function testVarcharLengthChange() {
        $tbl = new TableDef('varCharLengthChange');
        $tbl->setColumn('a', 'varchar(20)');
        $tbl->exec(static::$db);

        $tbl->setColumn('a', 'varchar(30)');
        $tbl->exec(static::$db);

        static::$db->reset();
        $cols = static::$db->fetchColumnDefs($tbl->getTable());

        $this->assertSame(30, $cols['a']['maxLength']);

    }

    /**
     * Tests a bug when altering an existing table after creating a new table.
     */
    public function testAlterAfterCreate() {
        for ($i = 0; $i < 2; $i++) {
            $tbl1 = new TableDef("table1_$i");
            $tbl1->setColumn('a', 'varchar(20)');
            $tbl1->exec(static::$db);

            $tbl2 = new TableDef('table2');
            $tbl2->setColumn('a', 'varchar(20)');
            $tbl2->exec(static::$db);

            static::$db->reset();
        }

        // This test is just to check DB exceptions.
        $this->assertTrue(true);
    }
}
