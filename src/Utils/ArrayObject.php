<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Utils;


class ArrayObject extends \ArrayObject {
    public function __construct(array $input = null) {
        parent::__construct((array)$input, \ArrayObject::ARRAY_AS_PROPS);
    }
}
