<?php
namespace sashalenz\Cassandra;

use Orchestra\Testbench\TestCase;

abstract class BaseTestCase extends TestCase
{

    public function setUp()
    {
        parent::setUp();
    }

    public function tearDown()
    {
        parent::tearDown();
    }
}
