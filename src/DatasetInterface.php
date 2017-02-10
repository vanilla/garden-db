<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

interface DatasetInterface extends \Traversable, \JsonSerializable {
    public function getOffset();
    public function setOffset($offset);

    public function getLimit();
    public function setLimit($limit);

    public function getPage();
    public function setPage($page);

    public function getOrder();
    public function setOrder($order);
}