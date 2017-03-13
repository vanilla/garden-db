<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\Sqlite;

use Garden\Db\Db;
use Garden\Db\SqliteDb;
use PDO;

trait SqliteTestTrait {
    protected abstract static function getPx();

    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        $px = static::getPx();

        if (getenv('TRAVIS')) {
            $path = ':memory:';
        } else {
            $path = __DIR__."/../cache/{$px}test.sqlite";
        }
        $pdo = new PDO("sqlite:$path", null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $db = new SqliteDb($pdo);
        $db->setPx($px);

        return $db;
    }
}