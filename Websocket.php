<?php
namespace Tanbolt\Websocket;

use Tanbolt\Websocket\Swoole\Swoole;
use Tanbolt\Websocket\Workerman\Workerman;

/**
 * Class Websocket
 * @package Tanbolt\Websocket
 * WebSocket 入口类
 */
class Websocket
{
    /**
     * @var array
     */
    protected $options = [];

    /**
     * @var EngineInterface
     */
    protected $engine = null;

    /**
     * Websocket constructor.
     * @param array $options
     * @throws \ErrorException
     */
    public function __construct(array $options = [])
    {
        $options = array_merge([
            'engine' => 'swoole',
            'host' => '127.0.0.1',
            'port' => 9586,
            'worker_num' => 1,
            'ping_interval' => 30000,
            'disable_polling' => false,
        ], array_combine(array_map(function($key) {
            return strtolower($key);
        }, array_keys($options)), $options));
        // windows 仅供测试用, 只能启动一个 worker
        if (DIRECTORY_SEPARATOR === '\\') {
            $options['worker_num'] = 1;
        }
        $this->options = $options;
        $host = $options['host'];
        $port = $options['port'];
        $engine = $options['engine'];
        unset($options['host'], $options['port'], $options['engine']);
        if ($engine === 'swoole') {
            $this->engine = new Swoole($host, $port, $options);
        } elseif ($engine === 'workerman') {
            $this->engine = new Workerman($host, $port, $options);
        } elseif ($engine && class_exists($engine) && $engine instanceof EngineInterface){
            $this->engine = new $engine($host, $port, $options);
        } else {
            throw new \ErrorException('Engine not exist');
        }
        Event::init();
    }

    /**
     * 设置 server 启动回调 (workerman 不支持, 会自动忽略)
     * @param callable $callback
     * @return $this
     */
    public function onBoot(callable $callback)
    {
        Event::$onBoot = $callback;
        return $this;
    }

    /**
     *  Worker 进程启动回调
     * @param callable $callback
     * @return $this
     */
    public function onWorkerStart(callable $callback)
    {
        Event::$onWorkerStart = $callback;
        return $this;
    }

    /**
     * Worker 进程重载回调
     * @param callable $callback
     * @return $this
     */
    public function onWorkerReload(callable $callback)
    {
        Event::$onWorkerReload = $callback;
        return $this;
    }

    /**
     * Worker 进程退出回调
     * @param callable $callback
     * @return $this
     */
    public function onWorkerStop(callable $callback)
    {
        Event::$onWorkerStop = $callback;
        return $this;
    }

    /**
     * Socket 客户端连接回调
     * @param callable $callback
     * @return $this
     */
    public function onOpen(callable $callback)
    {
        Event::$onOpen = $callback;
        return $this;
    }

    /**
     * Socket 收到消息回调
     * @param callable $callback
     * @return $this
     */
    public function onMessage(callable $callback)
    {
        Event::$onMessage = $callback;
        return $this;
    }

    /**
     * Socket 收到约定消息回调
     * @param string $event
     * @param callable $callback
     * @return $this
     */
    public function on($event, callable $callback)
    {
        if (!isset(Event::$listeners[$event])) {
            Event::$listeners[$event] = [];
        }
        Event::$listeners[$event][] = $callback;
        return $this;
    }

    /**
     * Socket 客户端断开回调
     * 在 open 时发生异常导致的连接断开, 不会触发该回调
     * @param callable $callback
     * @return $this
     */
    public function onClose(callable $callback)
    {
        Event::$onClose = $callback;
        return $this;
    }

    /**
     * Socket 处理消息发生异常的回调, 异常可能发生在 open 和 emit 期间
     * 1. open 时异常, 会自动断开连接, 但不调用 onClose 回调
     * 2. emit 时异常, 仅调用 onError 回调, 不会断开连接
     * @param callable $callback
     * @return $this
     */
    public function onError(callable $callback)
    {
        Event::$onError = $callback;
        return $this;
    }

    /**
     * 若为集群, 节点出现故障无法连接时触发
     * 由于去中心化, 所有该回调会在每一个可用节点触发, 如果需要做处理, 需注意去重
     * @param callable $callback
     * @return $this
     */
    public function onNodeOffline(callable $callback)
    {
        Event::$onNodeOffline = $callback;
        return $this;
    }

    /**
     * 启动 Server
     * @return $this
     */
    public function start()
    {
        $this->engine->start();
        return $this;
    }








    /**
     * 获取当前 Server 状态
     * @return array
     */
    public function status()
    {
        return $this->engine->status();
    }

    /**
     * 获取当前服务器客户端连接数
     * @return int
     */
    public function clientCount()
    {
        return 10;
    }

    /**
     * 获取当前服务器客户端 连接列表
     * @param int $fd
     * @param int $row
     * @return array
     */
    public function clients($fd, $row = 100)
    {
        return [];
    }

    /**
     * 获取当前 server 的内网ip
     * @return string
     */
    public static function getServerAddr()
    {
        if (DIRECTORY_SEPARATOR === '\\') {
            return getHostByName(getHostName());
        }
        exec('/sbin/ifconfig', $output, $code);
        if ($code === 0) {
            $output = join('', $output);
            if (preg_match('/inet\s([\d.]+)/', $output, $match)) {
                return $match[1];
            }
        }
        return null;
    }
}
