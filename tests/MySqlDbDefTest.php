<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Tests\Db;

use Garden\Db\MySqlDb;

/**
 * Run the {@link DbDefTest} against {@link MySqlDb}.
 */
class MySqlDbDefTest extends DbDefTest {
    /**
     * Create the {@link MySqlDb}.
     *
     * @return \Garden\Db\MySqlDb Returns the new database connection.
     */
    protected static function createDb() {
        $db = new MySqlDb(new \PDO(
            "mysql:host=127.0.0.1;dbname=phpunit_garden",
            'travis'
        ));

        $db->setPx('gdndef_');


        return $db;
    }
}
