<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Db;

trait DbTestTrait {
    /**
     * @var Db
     */
    protected static $db;

    /**
     * Set up the db link for the test cases.
     */
    public static function setUpBeforeClass() {
        // Drop all of the tables in the database.
        self::$db = static::createDb();

        $tables = self::$db->getAllTables();
        foreach ($tables as $table) {
            self::$db->dropTable($table);
        }
    }

    /**
     * Create the database for this test class.
     *
     * @return Db Returns a new database.
     */
    protected function createDb() {
        return null;
    }
}