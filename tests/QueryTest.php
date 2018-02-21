<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Db;
use Garden\Db\Query;
use PHPUnit\Framework\TestCase;

/**
 * Test the {@link Query} class.
 */
class QueryTest extends TestCase {
    /**
     * Test a basic bracketed expression.
     */
    public function testBasicBrackets() {
        $qry = new Query();

        $qry->beginAnd()
            ->addWhere('foo', 'bar')
            ->addWhere('bar', 'baz')
            ->end();

        $where = $qry->toArray()['where'];
        $this->assertEquals(
            [[Db::OP_AND => ['foo' => 'bar', 'bar' => 'baz']]],
            $where
        );
    }

    /**
     * {link Query::beginAnd()} and {link Query::beginOr()} should be similar.
     */
    public function testBeginOr() {
        $qry = new Query();

        $qry->beginOr()
            ->addWhere('foo', 'bar')
            ->end();

        $this->assertEquals(
            [[Db::OP_OR => ['foo' => 'bar']]],
            $qry->toArray()['where']
        );
    }

    /**
     * You should be able to add where expressions after calling {@link Query::end()}.
     */
    public function testWhereAfterEnd() {
        $qry = new Query();

        $qry->beginAnd()
            ->addWhere('foo', 'bar')
            ->end()
            ->addWhere('bar', 'baz');

        $this->assertEquals(
            [[Db::OP_AND => ['foo' => 'bar']], 'bar' => 'baz'],
            $qry->toArray()['where']
        );
    }

    /**
     * You should be able to nest where groups.
     */
    public function testNestedWhere() {
        $qry = new Query();

        $qry->beginAnd()
            ->addWhere('foo', 'bar')
            ->beginOr()
            ->addWhere('bar', 'baz')
            ->end()
            ->addWhere('baz', 'bam')
            ->end();

        $this->assertEquals(
            [[Db::OP_AND => ['foo' => 'bar', [Db::OP_OR => ['bar' => 'baz']]]], 'baz' => 'bam'],
            $qry->toArray()['where']
        );
    }

    /**
     * You should be able to pass multiple where values as an array to {@link Query::where()}.
     */
    public function testWhereArray() {
        $qry = new Query();

        $qry->addWheres(['foo' => 'bar', 'bar' => 'baz']);

        $this->assertEquals(
            ['foo' => 'bar', 'bar' => 'baz'],
            $qry->toArray()['where']
        );
    }

    /**
     * Additional calls to {@link Query::where()} on the same column should merge values.
     */
    public function testWhereMerge() {
        $qry = new Query();

        $qry->addWhere('foo', 'bar')
            ->addWhere('foo', [Db::OP_NEQ => 'baz'])
            ->addWhere('bar', [Db::OP_NEQ => 'bam'])
            ->addWhere('bar', 'qux');

        $this->assertEquals(
            [
                'foo' => [Db::OP_EQ => 'bar', Db::OP_NEQ => 'baz'],
                'bar' => [Db::OP_NEQ => 'bam', Db::OP_EQ => 'qux']
            ],
            $qry->toArray()['where']
        );
    }

    /**
     * Numeric arrays get converted to explicit IN statement when merging WHERE clauses.
     */
    public function testWhereMergeSimpleIn() {
        $qry = new Query();

        $qry->addWhere('foo', [1, 2])
            ->addWhere('foo', 'bar')
            ->addWhere('bar', 'baz')
            ->addWhere('bar', [3, 4]);

        $this->assertEquals(
            [
                'foo' => [Db::OP_IN => [1, 2], Db::OP_EQ => 'bar'],
                'bar' => [Db::OP_EQ => 'baz', Db::OP_IN => [3, 4]]
            ],
            $qry->toArray()['where']
        );
    }
}
