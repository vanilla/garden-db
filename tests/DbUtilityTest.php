<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Db;


/**
 * Test some aspects of the {@link Db} class that don't require a database connection.
 */
abstract class DbUtilityTest extends AbstractDbTest {
    /**
     * Test {@link Db::getType()}.
     *
     * @param string $type The type string.
     * @param array $expected The expected schema.
     * @dataProvider provideGetTypeTests
     */
    public function testGetType($type, $expected) {
        $schema = Db::typeDef($type);
        $this->assertEquals($expected, $schema);
    }

    public function provideGetTypeTests() {
        $r = [
            ['char(10)', ['type' => 'string', 'dbtype' => 'char', 'maxLength' => 10]],
            ['varchar(10)', ['type' => 'string', 'dbtype' => 'varchar', 'maxLength' => 10]],
            ['binary(10)', ['type' => 'string', 'dbtype' => 'binary', 'maxLength' => 10]],
            ['varbinary(10)', ['type' => 'string', 'dbtype' => 'varbinary', 'maxLength' => 10]],
            ['tinytext', ['type' => 'string', 'dbtype' => 'tinytext', 'maxLength' => 255]],
            ['text', ['type' => 'string', 'dbtype' => 'text', 'maxLength' =>  65535]],
            ['mediumtext', ['type' => 'string', 'dbtype' => 'mediumtext', 'maxLength' => 16777215]],
            ['longtext', ['type' => 'string', 'dbtype' => 'longtext', 'maxLength' => 4294967295]],

            ['bool', ['type' => 'boolean', 'dbtype' => 'bool']],

            ['byte', ['type' => 'integer', 'dbtype' => 'byte', 'maximum' => 127, 'minimum' => -128]],
            ['ubyte', ['type' => 'integer', 'dbtype' => 'byte', 'unsigned' => true, 'maximum' => 2**8 - 1, 'minimum' => 0]],
            ['int', ['type' => 'integer', 'dbtype' => 'int', 'maximum' => 2147483647, 'minimum' => -2147483648]],
            ['uint', ['type' => 'integer', 'dbtype' => 'int', 'unsigned' => true, 'maximum' => 2**32 - 1, 'minimum' => 0]],

            ['decimal(10, 2)', ['type' => 'number', 'dbtype' => 'decimal', 'precision' => 10, 'scale' => 2]],
            ['numeric(10, 2)', ['type' => 'number', 'dbtype' => 'numeric', 'precision' => 10, 'scale' => 2]],

            ["enum('foo', bar)", ['type' => 'string', 'dbtype' => 'enum', 'enum' => ['foo', 'bar']]]
        ];

        return array_column($r, null, 0);
    }
}
