<?php 

namespace Tests\Fixtures;

/**
 * A plain third-party exception — retryable only when explicitly registered
 * via TransactionOptions::withRetryableExceptions() (tier-3).
 */
class RetryableAppException extends \RuntimeException {}