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
    }
}
