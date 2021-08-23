# swoole-jobs
 
* Distributed task processing system,similar to gearman,based on swoole
* High performance / dynamic multi woker process consumption queue to accelerate backend time consuming service
* There is no need to configure a crontab like gearman worker, swoole-jobs is responsible for managing all worker states
* Support for pushing queues by HTTP API（swoole http server） , does not depend on php-fpm

Original repo: https://github.com/kcloze/swoole-jobs

## 1. Explain

* Slower logic in web, such as statistical /email/ SMS / picture processing, etc.
* Support redis/rabbitmq/zeromq or any other queue message store.
* It is more stable and faster than the Yii / laravel framework itself.
* With yii2/phalcon/yaf/ThinkPHP5 integration example, other frameworks can refer to src/Action code.
* [yii2 demo](https://github.com/kcloze/swoole-jobs-yii2)
* [ThinkPHP5 demo](https://github.com/kcloze/swoole-jobs-tp5)

## 2. Characteristic

* job scheduling component based on swoole; distributed task processing system similar to gearman;

* redis/rabbitmq/zeromq and any other queue message store (currently only redis/rabbitmq).

* use swoole process to realize multi process management, the number of processes can be configured, and the worker process will automatically pull up after exiting.

* the number of cycles of child processes can be configured to prevent memory leakage from business code; the default stop command will wait for the child process to exit smoothly.

* support topic features, different job binding different topic;

* each topic starts the corresponding number of sub processes to eliminate the interaction between different topic.

* according to the queue backlog, the sub process starts the process dynamically, and the number of the largest sub processes can be configured.

* support composer, which can be integrated with any framework;

* log file automatic cutting, default maximum 100M, up to 5 log files, prevent log brush full disk;

* backlog, support for nail robot and other news alerts.


## 4. Install

#### 4.1 composer
```
git clone https://github.com/Spywer/swoole-jobs.git
cd swoole-jobs
composer install
