<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\Sqlite;

use Garden\Db\SqliteDb;
use Garden\Db\Tests\DbDefTest;

/**
 * Run the {@link DbDefTest} against {@link SqliteDb}.
 */
class SqliteDbDefTest extends DbDefTest {
    /**
     * Create the {@link SqliteDb}.
     *
     * @return \Garden\Db\SqliteDb Returns the new database connection.
     */
    protected static function createDb() {
        if (getenv('TRAVIS')) {
            $path = ':memory:';
        } else {
            $path = __DIR__.'/../cache/dbdeftest.sqlite';
        }

        $db = new SqliteDb(new \PDO("sqlite:$path"));

        $db->setPx('tst_');

        return $db;
    }
}
