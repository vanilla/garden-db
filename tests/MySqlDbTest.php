<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Tests\Db;


use Garden\Db\Db;
use Garden\Db\MySqlDb;
use PDO;

/**
 * Exectute the {@link DbTest} tests against MySQL.
 */
class MySqlDbTest extends DbTest {

    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        $db = new MySqlDb(new PDO(
            "mysql:host=127.0.0.1;dbname=phpunit_garden",
            'travis',
            null,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        ));

        return $db;
    }
}
