<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\Sqlite;

use Garden\Db\Db;
use Garden\Db\SqliteDb;
use Garden\Db\Tests\DbTest;
use PDO;

/**
 * Exectute the {@link DbTest} tests against the {@link SqliteDb}.
 */
class SqliteDbTest extends DbTest {
    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        if (getenv('TRAVIS')) {
            $path = ':memory:';
        } else {
            $path = __DIR__.'/../cache/dbtest.sqlite';
        }
        $pdo = new PDO("sqlite:$path", null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

        $db = new SqliteDb($pdo);

        return $db;
    }
}
