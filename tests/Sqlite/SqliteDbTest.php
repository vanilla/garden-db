<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\Sqlite;

use Garden\Db\Tests\DbTest;

/**
 * Execute the {@link DbTest} tests against the {@link SqliteDb}.
 */
class SqliteDbTest extends DbTest {
    use SqliteTestTrait;
}
