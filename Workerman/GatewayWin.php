<?php
namespace Tanbolt\Websocket\Workerman;

use Workerman\Worker;
use Tanbolt\Websocket\Binary;
use Tanbolt\Websocket\Websocket;
use Tanbolt\Websocket\Context\Dispatch;
use Workerman\Connection\TcpConnection;

class GatewayWin extends Worker
{
    use Dispatch;

    /**
     * win 单进程不需要这个
     */
    protected function makeUnixSocketClient()
    {

    }

    /**
     * 载入 webSocket 对象
     * @param Websocket $websocket
     * @return $this
     */
    public function initWebsocket(Websocket $websocket)
    {
        $this->websocket = $websocket;
        $this->onWorkerStart = function (Worker $worker) {
            $this->onWorkerStart($worker->id);
        };
        $this->onWorkerReload = function (Worker $worker) {
            $this->onWorkerReload($worker->id);
        };
        $this->onWorkerStop = function (Worker $worker) {
            $this->onWorkerStop($worker->id);
        };
        $this->onOpen = function (TcpConnection $connection) {
            $this->onOpen($connection->worker->id, $connection->id, $connection->httpRequest);
        };
        $this->onMessage = function (TcpConnection $connection, $data) {
            $this->onMessage($connection->worker->id, $connection->id, $data, $connection->webSocketBinary);
        };
        $this->onClose = function () {

        };
        return $this;
    }
}
