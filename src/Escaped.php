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

    public function __construct($identifier) {
        $this->identifier = $identifier;
        parent::__construct('%s');
    }

    public function getValue(Db $db, ...$args) {
        $args = [$db->escape($this->identifier)];


        return parent::getValue($db, ...$args);
    }

    /**
     * Get the identifier.
     *
     * @return array|string Returns the identifier.
     */
    public function getIdentifier() {
        return $this->identifier;
    }

    /**
     * Set the identifier.
     *
     * @param array|string $identifier
     * @return $this
     */
    public function setIdentifier($identifier) {
        $this->identifier = $identifier;
        return $this;
    }
}
