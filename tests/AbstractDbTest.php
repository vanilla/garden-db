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
 * The base class for database tests.
 */
abstract class AbstractDbTest extends \PHPUnit_Framework_TestCase {
    /**
     * @var Db The database connection for the tests.
     */
    protected static $db;

    /// Methods ///

    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        return null;
    }

    /**
     * Get the database def.
     *
     * @return TableDef Returns the db def.
     */
    protected static function createDbDef() {
        return new TableDef(self::$db);
    }

    /**
     * Set up the db link for the test cases.
     */
    public static function setUpBeforeClass() {
        // Drop all of the tables in the database.
        $db = static::createDb();
        $tables = $db->getTableNames();
        array_map([$db, 'dropTable'], $tables);

        self::$db = $db;
    }

    /**
     * Assert that two table definitions are equal.
     *
     * @param string $tablename The name of the table.
     * @param array $expected The expected table definition.
     * @param array $actual The actual table definition.
     * @param bool $subset Whether or not expected can be a subset of actual.
     */
    public function assertDefEquals($tablename, $expected, $actual, $subset = true) {
        $this->assertEquals($expected['name'], $actual['name'], "Table names are not equal.");


        $colsExpected = $expected['columns'];
        $colsActual = $actual['columns'];

        if ($subset) {
            $colsActual = array_intersect_key($colsActual, $colsExpected);
        }
        $this->assertEquals($colsExpected, $colsActual, "$tablename columns are not the same.");

        $ixExpected = (array)$expected['indexes'];
        $ixActual = (array)$actual['indexes'];

        $isExpected = [];
        foreach ($ixExpected as $ix) {
            $type = $ix['type'] ?: Db::INDEX_IX;
            $isExpected[] = $type.'('.implode(', ', $ix['columns']).')';
        }
        sort($isExpected);

        $isActual = [];
        foreach ($ixActual as $ix) {
            $type = $ix['type'] ?: Db::INDEX_IX;
            $isActual[] = $type.'('.implode(', ', $ix['columns']).')';
        }

        if ($subset) {
            $isActual = array_intersect($isActual, $isExpected);
        }
        sort($isActual);
        $this->assertEquals($isExpected, $isActual, "$tablename indexes are not the same.");
    }
}
