<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license Proprietary
 */

namespace Garden\Db;

/**
 * Represents an escaped identifier.
 */
class Escaped extends Literal {
    private $identifier;

    public function __construct(...$identifier) {
        if (empty($identifier)) {
            throw new \InvalidArgumentException("The identifier cannot be empty.", 500);
        }
        $this->identifier = $identifier;
        parent::__construct('%s');
    }

    public function getValue(Db $db, ...$args) {
        $escaped = implode('.', array_map([$db, 'escape'], $this->identifier));

        return parent::getValue($db, $escaped);
    }
}
