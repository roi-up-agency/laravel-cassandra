<?php

namespace RoiupAgency\Cassadra\Schema\Grammars;

/**
 * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html
 */
use Illuminate\Support\Fluent;
use RoiupAgency\Cassadra\Connection;
use RoiupAgency\Cassadra\Schema\Blueprint;

/**
 * User: vhnvn
 * Date: 5/30/17
 * Time: 5:20 PM
 */
class Grammar extends \Illuminate\Database\Schema\Grammars\Grammar
{
    /**
     * The possible column modifiers.
     *
     * @var array
     */
    protected $modifiers = [];
    /**
     * The possible column serials.
     *
     * @var array
     */
    protected $serials = ['bigint', 'integer'];

    /**
     * Compile the query to determine the list of tables.
     *
     * @return string
     */
    public function compileTableExists()
    {
        return 'select * from system_schema.tables where keyspace_name = ? and table_name = ?';
    }

    /**
     * Compile the query to determine the list of columns.
     *
     * @return string
     */
    public function compileColumnListing()
    {
        return 'select column_name from system_schema.columns where keyspace_name = ? and table_name = ?';
    }

    /**
     * Compile a create table command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @param  \Illuminate\Database\Connection $connection
     * @return string
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command, Connection $connection)
    {
        $sql = $this->compileCreateTable(
            $blueprint, $command, $connection
        );
        return $this->compileOptions(
            $sql, $connection, $blueprint
        );
    }

    /**
     * Create the main create table clause.
     *
     *
     * @ref http://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_table_r.html
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @param  \Illuminate\Database\Connection $connection
     * @return string
     */
    protected function compileCreateTable($blueprint, $command, $connection)
    {
        if ($blueprint->temporary) {
            throw new \RuntimeException("Cassandra temporary table not supported");
        }

        $columns_definitions = $this->getColumns($blueprint);

        $index_definitions = $this->getIndexDefinitions($blueprint);

        return sprintf('create table %s (%s)',
            $this->wrapTable($blueprint),
            implode(', ', array_merge($columns_definitions, $index_definitions))
        );
    }

    protected function getIndexDefinitions($blueprint)
    {
        $definitions = [];

        if (isset($blueprint->primary_columns)) {
            $primary_columns = $blueprint->primary_columns;
            if (!is_array($primary_columns)) {
                $primary_columns = [$primary_columns];
            }
            $definitions [] = sprintf('PRIMARY KEY (%s)', implode(', ', array_map(function ($x) {
                if (is_array($x)) {
                    return '(' . implode(',', array_map([$this, 'wrapValue'], $x)) . ')';
                }
                return $this->wrapValue($x);
            }, $primary_columns)));
        }

        return $definitions;
    }

    /**
     * Append the engine specifications to a command.
     *
     * @param  string $sql
     * @param  \Illuminate\Database\Connection $connection
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @return string
     */
    protected function compileOptions($sql, Connection $connection, Blueprint $blueprint)
    {
        if (isset($blueprint->options)) {
            $options = $blue->options;
            if (count($options)) {
                $with = [];
                foreach ($options as $name => $option) {
                    $with .= $name . '=' . json_encode($option);
                }
                return $sql . ' WITH ' . implode(' AND ', $with);
            }
        }
        return $sql;
    }

    /**
     * Compile an add column command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @return string
     */
    public function compileAdd(Blueprint $blueprint, Fluent $command)
    {
        $columns = $this->prefixArray('add', $this->getColumns($blueprint));
        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a plain index key command.
     *
     * @ref http://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_index_r.html
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @return string
     */
    public function compileIndex(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('create index %s ON %s (%s) %s%s',
            $this->wrap($blueprint->getTable() . '_' . $command->index),
            $this->wrapTable($blueprint),
            implode(',', array_map([$this, 'wrapValue'], $command->columns)),
            $command->algorithm ? ' using ' . $command->algorithm : '',
            $command->options ? ' WITH options = ' . json_encode($command->options) : ''
        );
    }

    /**
     * Compile a drop table command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @return string
     */
    public function compileDrop(Blueprint $blueprint, Fluent $command)
    {
        return 'drop table ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop table (if exists) command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @return string
     */
    public function compileDropIfExists(Blueprint $blueprint, Fluent $command)
    {
        return 'drop table if exists ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop column command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @return string
     */
    public function compileDropColumn(Blueprint $blueprint, Fluent $command)
    {
        $columns = $this->prefixArray('drop', $this->wrapArray($command->columns));
        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a drop index command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint $blueprint
     * @param  \Illuminate\Support\Fluent $command
     * @return string
     */
    public function compileDropIndex(Blueprint $blueprint, Fluent $command)
    {
        $index = $this->wrap($blueprint->getTable() . '_' . $command->index);
        return "drop index {$index}";
    }


    const TYPE_MAP = [
        'integer' => 'int',
    ];

    /**
     * Get the SQL for the column data type.
     *
     * @param  \Illuminate\Support\Fluent $column
     * @return string
     */
    protected function getType(Fluent $column)
    {
        $type = $column->type;
        if (array_key_exists($type, static::TYPE_MAP)) {
            return static::TYPE_MAP[$type];
        }
        return $type;
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string $value
     * @return string
     */
    protected function wrapValue($value)
    {
        if ($value !== '*') {
            return '"' . str_replace('"', '""', $value) . '"';
        }
        return $value;
    }
}