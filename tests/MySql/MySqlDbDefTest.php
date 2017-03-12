<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\MySql;

use Garden\Db\Tests\DbDefTest;

/**
 * Run the {@link DbDefTest} against {@link MySqlDb}.
 */
class MySqlDbDefTest extends DbDefTest {
    use MySqlTestTrait;
}
