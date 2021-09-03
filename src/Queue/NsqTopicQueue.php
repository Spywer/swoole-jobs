<?php

// Powered by Spywer

namespace Kcloze\Jobs\Queue;

use Kcloze\Jobs\Queue\BaseTopicQueue;
use Kcloze\Jobs\JobObject;
use Kcloze\Jobs\Logs;
use Kcloze\Jobs\Serialize;
use Kcloze\Jobs\Utils;

use OneNsq\Client;

class NsqTopicQueue extends BaseTopicQueue
{
    private $config = null;
    private $logger = null;

    private $client = null;
    private $payload  = null;
	
	private $ping = null;

    public function __construct($config, Logs $logger)
    {
        $this->config  = $config;
        $this->logger  = $logger;
		
		$this->ping = $this->apiClient('GET', 'ping') == 'OK' ? true : false;

        $conf = [
            'msg_timeout' => isset($config['client']) ? $config['client']['msg_timeout'] : 6000,
            'heartbeat_interval' => isset($config['client']) ? $config['client']['heartbeat_interval'] : 1600,
        ];

        $this->client = new Client($this->config['protocol'].'://'.$this->config['host'].':'.$this->config['port'], $conf);
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
		
		try {

            $this->client->publish($topic, Serialize::serialize($job, $serializeFunc), $delay);
		
		} catch (\Exception $e) {

            $this->ping = false;

			return '';
		}

        return $job->uuid ?? '';
    }

    public function pop($topic, $unSerializeFunc='php')
    {
        if (!$this->isConnected()) {
            return NULL;
        }

        $subscriber = $this->client->subscribe($topic, $topic);

        $this->payload = $subscriber->current();

        if(isset($this->payload->id)) {

            $unSerializeFunc=Serialize::isSerial($this->payload->msg) ? 'php' : 'json';
            return !empty($this->payload->msg) ? Serialize::unSerialize($this->payload->msg, $unSerializeFunc) : null;
        }

        return NULL;
    }

    public function ack(): bool
    {
        if($this->client) {

            if($this->payload) {

                $this->client->touch($this->payload->id);
                $this->client->finish($this->payload->id);

                return true;
            }
        }

        $this->ping = false;

        return false;
    }

    public function len($topic): int
    {
        $response = $this->apiClient('GET', 'stats', ['topic' => $topic, 'channel' => $topic, 'format' => 'json']);

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

        if($this->client) {
            return $this->client->close();
        }

        $this->ping = false;

        return false;
    }

    public function isConnected()
    {
        try {

            return $this->ping;

        } catch (\Throwable $e) {
            Utils::catchError($this->logger, $e);
        } catch (\Exception $e) {
            Utils::catchError($this->logger, $e);
        }

        return false;
    }

    protected function apiClient($method = 'GET', $path = NULL, $param = array(), $post = array())
    {
        $options = [];

        if($param) { $query = '?'.http_build_query($param); } else { $query = ''; }

        if($method == 'POST' && $post) {
            $options = array_merge_recursive($options, ['form_params' => $post]);
        }

        $client = new \GuzzleHttp\Client([
            'base_uri' => $this->config['api']['host'].':'.$this->config['api']['port'],
            'verify' => false,
            'timeout' => 5.0
        ]);
		
		$res = $client->request($method, $path.$query, $options);

        try {

            if($res->getStatusCode() == 200) {

				if(isset($param['format']) && $param['format'] == 'json') {
					return json_decode($res->getBody()->getContents());
				} else {
					return $res->getBody()->getContents();
				}
            }

        } catch (\Exception $e) {

            return false;
        }

        return false;
    }
}
