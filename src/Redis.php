<?php
/*LICENSE
+-----------------------------------------------------------------------+
| SilangPHP Framework                                                   |
+-----------------------------------------------------------------------+
| This program is free software; you can redistribute it and/or modify  |
| it under the terms of the GNU General Public License as published by  |
| the Free Software Foundation. You should have received a copy of the  |
| GNU General Public License along with this program.  If not, see      |
| http://www.gnu.org/licenses/.                                         |
| Copyright (C) 2020. All Rights Reserved.                              |
+-----------------------------------------------------------------------+
| Supports: http://www.github.com/silangtech/SilangPHP                  |
+-----------------------------------------------------------------------+
*/
declare(strict_types=1);
namespace SilangPHP\Queue;
class Redis
{
    protected $prefix = 'redisQueue';
    protected $redis = null;
    protected $key = '';

    public function __construct($queue, $config = [])
    {
        $this->key = $this->prefix . $queue;
        $this->redis = new \Redis();
        $this->redis->connect($config['host'], $config['port'], $config['timeout']);
        if(!empty($config['auth']))
        {
            $this->redis->auth($config['auth']);
        }
        if(!empty($config['db']))
        {
            $this->redis->select($config['db']);
        }
    }

    public function getTask($name='')
    {
        return $this->redis->rpop($this->key.$name);
    }

    public function addTask($name, $time, $data)
    {
        $this->redis->lpush($this->key.$name,$data);
    }

    public function run(callable $func = null)
    {
        if($func)
        {
            //每次只取一条任务
            while($task = $this->getTask())
            {
                $func($task);
            }
        }else{
            return $this->getTask();
        }
        return true;
    }

}