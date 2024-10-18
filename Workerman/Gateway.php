<?php
namespace Tanbolt\Websocket\Workerman;

use Tanbolt\Websocket\Websocket;
use Workerman\Worker;
use Tanbolt\Websocket\Context\Dispatch;

class Gateway extends Worker
{
    use Dispatch;

    protected function initEvent(Websocket $websocket, $unixSocket = null)
    {
        $this->unixSocket = $unixSocket;
        if (!$unixSocket) {

            return $this;
        }



    }


    protected function makeUnixSocketClient()
    {

    }
}
