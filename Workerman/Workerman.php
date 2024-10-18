<?php
namespace Tanbolt\Websocket\Workerman;

use Workerman\Worker;
use Tanbolt\Websocket\Websocket;

class Workerman
{
    /**
     * @var bool
     */
    protected $win = false;

    /**
     * @var GatewayWin
     */
    protected $gateway;

    /**
     * Workerman constructor.
     * @param Websocket $websocket
     * @param $host
     * @param $port
     * @param array $options
     */
    public function __construct(Websocket $websocket, $host, $port, array $options = [])
    {
        if(!class_exists('Protocols\WsProtocol')) {
            class_alias(
                __NAMESPACE__ . '\WsProtocol',
                'Protocols\WsProtocol'
            );
        }
        $this->win = DIRECTORY_SEPARATOR === '\\';
        $this->initServer($websocket, $host, $port, $options);
    }

    /**
     * 初始化 Server (Register, Gateway, Business)
     * @param Websocket $websocket
     * @param $host
     * @param $port
     * @param array $options
     * @return $this
     */
    protected function initServer(Websocket $websocket, $host, $port, array $options = [])
    {
        // pid_file pid_log log_file
        $daemonize = isset($options['daemonize']) && $options['daemonize'];
        if ($daemonize && isset($options['log_file']) && !empty($options['log_file'])) {
            Worker::$stdoutFile = $options['log_file'];
        }
        if (!isset($options['pid_file']) || !isset($options['pid_log'])) {
            $tmpDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR;
            if (!isset($options['pid_file'])) {
                $options['pid_file'] = $tmpDir.'wsocket_'.$host.'_'.$port.'.pid';
            }
            if (!isset($options['pid_log'])) {
                $options['pid_log'] = $tmpDir.'wsocket_'.$host.'_'.$port.'.log';
            }
        }
        Worker::$pidFile = $options['pid_file'];
        Worker::$logFile = $options['pid_log'];

        // win 仅供测试, 只启动一个进程, 直接使用 GatewayWin 对外提供 webSocket 服务即可
        if ($this->win) {
            $this->gateway = new GatewayWin('WsProtocol://'.$host.':'.$port);
            $this->gateway->count = 1;
            $this->gateway->name = (isset($options['name']) ? $options['name'] : 'WebSocket') . ' Gateway';
            if (isset($options['group'])) {
                $this->gateway->group = $options['group'];
            }
            if (isset($options['user'])) {
                $this->gateway->user = $options['user'];
            }
            $this->gateway->initWebsocket($websocket);
            return $this;
        }






//
//        $unixSocket = $win ? null : (isset($options['socket_file']) ? $options['socket_file'] : sys_get_temp_dir().'/wsbus.socket');
//
//
//
//        if ($win) {
//            return $this;
//        }
//
//
//        $worker_num = (isset($options['worker_num']) ? (int) $options['worker_num'] : 0) ?: 1;
//        // 调度进程 不能超过 worker_num, 默认为 worker_num 的 1/4, 最少一个
//        $reactor_num = isset($options['reactor_num']) ? (int) $options['reactor_num'] : 0;
//        $gateway->count = $reactor_num ? min($reactor_num, $worker_num) : max(1, $worker_num/4);
//
//
//        // Business
//
//
//        // Register


        return $this;
    }



    public function send($fd, $polling, $code, array $data = [])
    {
        if ($this->win) {
            $this->gateway->send($fd, $polling, $code, $data);
        }

    }



    /**
     * 启动服务
     */
    public function start()
    {
        Worker::runAll();
    }
}
