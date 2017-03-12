<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\Sqlite;

use Garden\Db\Tests\DbDefTest;

/**
 * Run the {@link DbDefTest} against {@link SqliteDb}.
 */
class SqliteDbDefTest extends DbDefTest {
    use SqliteTestTrait;
}
