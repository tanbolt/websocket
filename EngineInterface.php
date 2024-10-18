<?php
namespace Tanbolt\Websocket;

interface EngineInterface
{
    /**
     * EngineInterface constructor.
     * @param $server
     * @param $port
     * @param array $options
     */
    public function __construct($server, $port, array $options = []);

    /**
     * 启动 Server
     * @return mixed
     */
    public function start();

    /**
     * 获取当前 Server 状态
     * @return array
     */
    public function status();

    /**
     * 获取当前服务器客户端连接数
     * @return int
     */
    public function clientCount();

    /**
     * 获取当前服务器客户端 连接列表
     * @param int $fd
     * @param int $row
     * @return array
     */
    public function clients($fd, $row = 100);
}
