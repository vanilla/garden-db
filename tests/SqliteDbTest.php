<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Tests\Db;

use Garden\Db\Db;

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
            $path = __DIR__.'/cache/dbtest.sqlite';
        }

        $db = Db::create([
            'driver' => 'SqliteDb',
            'path' => $path,
        ]);

        return $db;
    }
}
