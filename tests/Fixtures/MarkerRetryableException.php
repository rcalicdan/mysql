<?php 

namespace Tests\Fixtures;

use Hibla\Sql\Exceptions\RetryableException;

/**
 * Implements the RetryableException marker directly — tier-1 retry, zero config needed.
 */
class MarkerRetryableException extends \RuntimeException implements RetryableException {}