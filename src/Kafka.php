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
class Kafka
{
    protected $prefix = 'kafkaQueue';
    protected $redis = null;
    protected $key = '';
    protected $conf;
    protected $producer;
    protected $consumer;
    protected $producer_topic = [];
    protected $consumer_topic = [];
    protected $consumer_topic_partition = [];

    public function __construct($queue = '', $config = [])
    {
        $this->key = $this->prefix . $queue;
        $this->conf = new \RdKafka\Conf();
        // $this->conf->set('log_level', (string)LOG_DEBUG);
        // $this->conf->set('debug', 'all');
        if(empty($config['groupid']))
        {
            $this->conf->set('group.id', $this->key);
        }else{
            $this->conf->set('group.id', $config['groupid']);
        }
        $this->topicConf = new \RdKafka\TopicConf();
        $this->topicConf->set('auto.commit.interval.ms', '100');
        $this->topicConf->set('offset.store.method', 'file');
        $this->topicConf->set('offset.store.path', sys_get_temp_dir());
        $this->topicConf->set('auto.offset.reset', 'earliest');

        $this->broker_str = $config['brokers'];
        $this->producer = new \RdKafka\Producer($this->conf);
        $this->producer->addBrokers($this->broker_str);
        $this->consumer = new \RdKafka\Consumer($this->conf);
        $this->consumer->addBrokers($this->broker_str);
    }

    public function new_comsumer_topic($name)
    {
        $this->consumer_topic[$name] = $this->consumer->newTopic($name,$this->topicConf);
    }

    public function new_producer_topic($name)
    {
        $this->producer_topic[$name] = $this->producer->newTopic($name,$this->topicConf);
    }

    public function getTask($name='' , $partition = 0 , $timeout = 1000)
    {
        if(!isset($this->consumer_topic[$name]))
        {
            $this->new_comsumer_topic($name);
        }
        if(!isset($this->consumer_topic_partition[$name."_".$partition]))
        {
	        $this->consumer_topic[$name]->consumeStart($partition, \RD_KAFKA_OFFSET_STORED);
            $this->consumer_topic_partition[$name."_".$partition] = $partition;
        }
        
        $msg = $this->consumer_topic[$name]->consume($partition, $timeout);
        // 为NUll没数据的情况
        if (null === $msg || $msg->err === \RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            return false;
        } elseif ($msg->err && $msg->err != \RD_KAFKA_RESP_ERR_NO_ERROR) {
            // return $msg->errstr();
            return false;
        } else {
            return $msg->payload;
        }
    }

    public function addTask($name, $payload, $partition = \RD_KAFKA_PARTITION_UA)
    {
        if(!isset($this->producer_topic[$name]))
        {
            $this->new_producer_topic($name);
        }
        // 这里的key先不加,直接使用value，使用上不大
        $this->producer_topic[$name]->produce($partition, 0, $payload);
        $this->producer->poll(0);
        $result = $this->producer->flush(10000);
    }

    public function stats()
    {
        $all = $this->consumer->metadata(true, NULL, 15 * 1000);
        $topics = $all->getTopics();
        foreach ($topics as $topic) 
        {
            $topicName = $topic->getTopic();
            if ($topicName == "__consumer_offsets") 
            {
                continue ;
            }
            $partitions = $topic->getPartitions();
            foreach ($partitions as $partition) 
            {
                $topPartition = new \RdKafka\TopicPartition($topicName, $partition->getId());
                echo "topic: ".$topPartition->getTopic()." - partition: ".$partition->getId()." - "."offset: ".$topPartition->getOffset().PHP_EOL;
            }
        }
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
        return false;
    }

}