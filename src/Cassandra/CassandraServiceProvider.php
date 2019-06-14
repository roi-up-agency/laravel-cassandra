<?php

namespace Cassandra;

use Illuminate\Support\ServiceProvider;
use RoiupAgency\Cassadra\Eloquent\Model;
use RoiupAgency\Cassadra\Queue\CassandraConnector;

class CassandraServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application events.
     */
    public function boot()
    {
        Model::setConnectionResolver($this->app['db']);

        Model::setEventDispatcher($this->app['events']);
    }

    /**
     * Register the service provider.
     */
    public function register()
    {
        // Add database driver.
        $this->app->resolving('db', function ($db) {
            $db->extend('cassandra', function ($config) {
                return new Connection($config);
            });
        });

        // Add connector for queue support.
        $this->app->resolving('queue', function ($queue) {
            $queue->addConnector('cassandra', function () {
                return new CassandraConnector($this->app['db']);
            });
        });
    }
}
