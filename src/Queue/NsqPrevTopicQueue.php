<?php

// Powered by Spywer
// For "nsq/nsq": "0.3" with PHP 7.4

namespace Kcloze\Jobs\Queue;

use Kcloze\Jobs\Queue\BaseTopicQueue;
use Kcloze\Jobs\JobObject;
use Kcloze\Jobs\Logs;
use Kcloze\Jobs\Serialize;
use Kcloze\Jobs\Utils;

use Nsq\Writer;
use Nsq\Envelope;
use Nsq\Subscriber;

class NsqPrevTopicQueue extends BaseTopicQueue
{
    private $config = null;
    private $logger = null;

    private $writer = null;
    private $subscriber = null;
    private $generator  = null;
    private $envelope  = null;
	
	private $ping = null;

    public function __construct($config, Logs $logger)
    {
        $this->config   = $config;
        $this->logger  = $logger;
		
		$this->ping = $this->apiClient('GET', 'ping') == 'OK' ? true : false;
		
		if(isset($config['producer']) && $config['producer'] == true) {
			$this->writer = new Writer($this->config['protocol'].'://'.$this->config['host'].':'.$this->config['port']);
		} else {
			$this->subscriber = new Subscriber($this->config['protocol'].'://'.$this->config['host'].':'.$this->config['port']);
		}
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
            $this->writer->dpub($topic, $delay, Serialize::serialize($job, $serializeFunc));
        } else {
            $this->writer->pub($topic, Serialize::serialize($job, $serializeFunc));
        }

        return $job->uuid ?? '';
    }

    public function pop($topic, $unSerializeFunc='php')
    {
        if (!$this->isConnected()) {
            return NULL;
        }

		$this->generator = $this->subscriber->subscribe($topic, $topic, 5);
		
		try {
			
			$this->envelope = $this->generator->current();
			
		} catch (\Exception $e) {
			
			$this->envelope = NULL;
		}

		if ($this->envelope instanceof Envelope) {
			
			$payload = $this->envelope->message->body;
			$unSerializeFunc=Serialize::isSerial($payload) ? 'php' : 'json';

			return !empty($payload) ? Serialize::unSerialize($payload, $unSerializeFunc) : null;
		}

        return NULL;
    }

    public function ack(): bool
    {
        if ($this->envelope instanceof Envelope) {
			$this->envelope->touch();
            $this->envelope->finish();
        }
		
		if($this->generator) {
			$this->generator->send(true);
			return true;
		}
		
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

        if($this->writer) {

            $this->writer->disconnect();
			
			return $this->writer->closed;

        } else if($this->subscriber) {

            $this->subscriber->disconnect();
			
			return $this->subscriber->closed;
        }
    }

    public function isConnected()
    {
        try {

            return $this->ping;

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
