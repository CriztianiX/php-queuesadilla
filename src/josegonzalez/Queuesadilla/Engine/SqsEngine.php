<?php

namespace josegonzalez\Queuesadilla\Engine;

use josegonzalez\Queuesadilla\Engine\Base;
use Aws\Sqs\SqsClient;

class SqsEngine extends Base
{
    protected $sqsSettings = [
        'version',
        'region',
        'queue',
        'credentials'
    ];

    /**
     * {@inheritDoc}
     */
    public function connect()
    {
        $settings = [];
        foreach ($this->sqsSettings as $key) {
            $settings[$key] = $this->config($key);
        }

        $this->connection = $this->sqsClient($settings);
        return (bool)$this->connection;
    }

    /**
     * {@inheritDoc}
     */
    public function acknowledge($item)
    {
        if (!parent::acknowledge($item)) {
            return false;
        }
        
        return $this->connection()->deleteMessage([
             'QueueUrl' => $item["queue"],
             'ReceiptHandle' => $item["id"]
        ]);
    }

    /**
     * {@inheritDoc}
     */
    public function reject($item)
    {
        return $this->acknowledge($item);
    }

    /**
     * {@inheritDoc}
     */
    public function pop($options = [])
    {
        $queue = $this->setting($options, 'queue');
        $response = $this->connection()->receiveMessage([
            'MaxNumberOfMessages' => 1,
            'QueueUrl' => $queue
        ]);

        $messages = $response->get('Messages');

        if (!$messages) {
            return null;
        }

        $item = $messages[0];
        $data = json_decode($item["Body"], true);

        $raise = [
            'id' => $item["ReceiptHandle"],
            'class' => $data['class'],
            'args' => $data['args'],
            'queue' => $data["queue"]
        ];

        return $raise;
    }

    /**
     * {@inheritDoc}
     */
    public function push($class, $options = [])
    {
        $queue = $this->setting($options, 'queue');
        $class["options"] = $options;
        $class["queue"] = $queue;

        return $this->sendMessage($class, $queue);
    }

    /**
     * {@inheritDoc}
     */
    public function queues()
    {
        return $this->connection()->listQueues();
    }

    /**
     * {@inheritDoc}
     */
    public function release($item, $options = [])
    {
        $queue = $this->setting($options, 'queue');
        return $this->sendMessage($item, $queue);
    }


    private function sendMessage($item, $queue)
    {
        return $this->connection()->sendMessage([
            'MessageBody' => json_encode($item),
            'QueueUrl' => $queue
        ]);
    }

    private function sqsClient($credentials)
    {
        return new SqsClient($credentials);
    }
}
