<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2018 Vanilla Forums Inc.
 * @license Proprietary
 */

namespace Garden\Db\Tests;


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
}
