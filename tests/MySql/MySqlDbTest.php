<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\MySql;

use Garden\Db\Tests\DbTest;

/**
 * Execute the {@link DbTest} tests against MySQL.
 */
class MySqlDbTest extends DbTest {
    use MySqlTestTrait;
}
