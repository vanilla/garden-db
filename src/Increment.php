<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license Proprietary
 */

namespace Garden\Db;

/**
 * Represents an incrementer calculation.
 *
 * Pass an instance of this class as a value to a database update call.
 */
class Increment extends Literal {
    /**
     * @var int The amount to increment.
     */
    private $inc;

    /**
     * Construct an increment.
     *
     * @param int $inc The amount to increment by.
     */
    public function __construct($inc = 1) {
        parent::__construct('%1$s %2$+d');
    }

    /**
     * {@inheritdoc}
     */
    public function getValue(Db $db, ...$args) {
        if (empty($args)) {
            throw new \InvalidArgumentException("Increment must specify the column to increment.");
        }

        $args[1] = $this->getInc();
        return parent::getValue($db, ...$args);
    }

    /**
     * Get the amount to increment.
     *
     * @return int Returns a number.
     */
    public function getInc() {
        return $this->inc;
    }

    /**
     * Set the amount to increment.
     *
     * @param int $inc
     * @return $this
     */
    public function setInc($inc) {
        $this->inc = $inc;
        return $this;
    }
}
