<?php

namespace Kcloze\Jobs\Queue;

use Kcloze\Jobs\Queue\BaseTopicQueue;
use Kcloze\Jobs\JobObject;
use Kcloze\Jobs\Logs;
use Kcloze\Jobs\Serialize;
use Kcloze\Jobs\Utils;

use Nsq\Producer;
use Nsq\Consumer;
use Nsq\Message;
use Nsq\Subscriber;

class NsqTopicQueue extends BaseTopicQueue
{
    private $config = null;
    private $logger = null;

    private $producer = null;
    private $consumer = null;
    private $generator  = null;
    private $message  = null;

    public function __construct($config, Logs $logger)
    {
        $this->config   = $config;
        $this->logger  = $logger;
    }

    protected function initProducer()
    {
        if(!$this->producer) {
            $this->producer = new Producer($this->config['protocol'].'://'.$this->config['host'].':'.$this->config['port']);
        }

        return $this->producer;
    }

    protected function initConsumer()
    {
        if(!$this->consumer) {
            $this->consumer = new Consumer($this->config['protocol'].'://'.$this->config['host'].':'.$this->config['port']);
        }

        return $this->consumer;
    }

    public static function getConnection(array $config, Logs $logger)
    {
        $connection = new self($config, $logger);
        return $connection;
    }

    public function push($topic, JobObject $job, $delayStrategy=1, $serializeFunc='php'): string
    {
        if (!$this->isConnected()) {
            return '';
        }

        $delay = $job->jobExtras['delay'] ?? 0;

        if($delay) {
            $this->initProducer()->dpub($topic, Serialize::serialize($job, $serializeFunc), $delay);
        } else {
            $this->initProducer()->pub($topic, Serialize::serialize($job, $serializeFunc));
        }

        return $job->uuid ?? '';
    }

    public function pop($topic, $unSerializeFunc='php')
    {
        if (!$this->isConnected()) {
            return false;
        }

        $subscriber = new Subscriber($this->initConsumer());
        $this->generator = $subscriber->subscribe($topic, $topic);
        $this->message = $this->generator->current();

        if ($this->message instanceof Message) {

            $payload = $this->message->body;

            $unSerializeFunc=Serialize::isSerial($payload) ? 'php' : 'json';

            return !empty($payload) ? Serialize::unSerialize($payload, $unSerializeFunc) : null;
        }

        return false;
    }

    public function ack(): bool
    {
        if ($this->message instanceof Message) {

            $this->message->touch();
            $this->message->finish();

            $this->generator->send(Subscriber::STOP);

            return true;
        }

        return false;
    }

    public function len($topic): int
    {
        $response = $this->apiClient('GET', 'stats', ['topic' => $topic, 'format' => 'json']);

        if($response) {

            if($response && isset($response->topics[0]) && isset($response->topics[0]->channels[0]) && isset($response->topics[0]->channels[0]->depth)) {

                return (int) $response->topics[0]->channels[0]->depth;

            }
        }

        return 0;
    }

    public function purge($topic)
    {
        if (!$this->isConnected()) {
            return 0;
        }

        $this->apiClient('POST', 'channel/empty', [
            'topic' => $topic, 'channel' => $topic
        ]);

        return true;
    }

    public function delete($topic)
    {
        if (!$this->isConnected()) {
            return 0;
        }

        $this->apiClient('POST', 'channel/delete', [
            'topic' => $topic, 'channel' => $topic
        ]);

        return true;
    }

    public function close()
    {
        if (!$this->isConnected()) {
            return false;
        }

        if($this->producer) {

            return $this->producer->disconnect();

        } else if($this->consumer) {

            return $this->consumer->disconnect();
        }
    }

    public function isConnected()
    {
        try {

            if($this->producer) {

                return $this->producer->isReady();

            } else if($this->consumer) {

                return $this->consumer->isReady();
            }

        } catch (\Throwable $e) {
            Utils::catchError($this->logger, $e);

            return false;
        } catch (\Exception $e) {
            Utils::catchError($this->logger, $e);

            return false;
        }

        return true;
    }

    protected function apiClient($method = 'GET', $path = NULL, $param = array(), $post = array())
    {
        $options = [];

        if($param) { $query = '?'.http_build_query($param); }

        if($method == 'POST' && $post) {
            $options = array_merge_recursive($options, ['form_params' => $post]);
        }

        $client = new \GuzzleHttp\Client([
            'base_uri' => $this->config['host'].':'.$this->config['port'],
            'verify' => false,
            'timeout' => 5.0
        ]);

        try {

            $res = $client->request($method, $path.$query, $options);

            if($res->getStatusCode() == 200) {

                return json_decode($res->getBody()->getContents());
            }

        } catch (\Exception $e) {

            return false;
        }

        return false;
    }
}